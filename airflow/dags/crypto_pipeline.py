"""
### Crypto Pulse pipeline

Runs every 5 minutes on top of `raw.trades`, which the streaming consumer fills in real time.

1. **check_freshness** – fail fast if any exchange feed has gone quiet.
2. **build_candles_1m** – OHLCV + VWAP + taker buy/sell volume per minute.
3. **build_spread_1m** – Coinbase vs Kraken price gap per minute, in basis points.
4. **build_market_stats_hourly** – hourly return, volatility, volume and buy pressure.
5. **capture_whale_trades** – keep every large trade forever (raw data is pruned).
6. **run_data_quality_checks** – results stored in `analytics.dq_results`.
7. **prune_raw_trades** – raw retention.

Every transform rebuilds a fixed lookback window, so reruns never double-count.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "crypto_db"  # defined via AIRFLOW_CONN_CRYPTO_DB in docker-compose.yml
EXCHANGES = ("coinbase", "kraken")
LOOKBACK_HOURS = int(os.getenv("CRYPTO_LOOKBACK_HOURS", "2"))
FRESHNESS_MINUTES = int(os.getenv("CRYPTO_FRESHNESS_MINUTES", "10"))
WHALE_USD = float(os.getenv("CRYPTO_WHALE_USD", "100000"))
RETENTION_DAYS = int(os.getenv("CRYPTO_RETENTION_DAYS", "3"))
MAX_SANE_SPREAD_BPS = 200  # a 2% gap between big exchanges almost always means bad data

WINDOW = "date_trunc('hour', now() - make_interval(hours => %(h)s))"
PARAMS = {"h": LOOKBACK_HOURS, "whale": WHALE_USD}

CANDLES_SQL = [
    f"DELETE FROM analytics.candles_1m WHERE bucket >= {WINDOW}",
    f"""
    INSERT INTO analytics.candles_1m
        (bucket, exchange, symbol, open, high, low, close, volume, notional, vwap,
         trades, buy_volume, sell_volume)
    SELECT date_trunc('minute', trade_ts),
           exchange,
           symbol,
           (array_agg(price ORDER BY trade_ts, length(trade_id), trade_id))[1],
           max(price),
           min(price),
           (array_agg(price ORDER BY trade_ts DESC, length(trade_id) DESC, trade_id DESC))[1],
           sum(size),
           sum(notional_usd),
           sum(notional_usd) / NULLIF(sum(size), 0),
           count(*),
           COALESCE(sum(size) FILTER (WHERE side = 'buy'), 0),
           COALESCE(sum(size) FILTER (WHERE side = 'sell'), 0)
    FROM raw.trades
    WHERE trade_ts >= {WINDOW}
    GROUP BY 1, 2, 3
    """,
]

SPREAD_SQL = [
    f"DELETE FROM analytics.spread_1m WHERE bucket >= {WINDOW}",
    f"""
    INSERT INTO analytics.spread_1m (bucket, symbol, coinbase_price, kraken_price, spread_bps)
    SELECT cb.bucket,
           cb.symbol,
           cb.close,
           kr.close,
           10000 * (cb.close - kr.close) / kr.close
    FROM analytics.candles_1m cb
    JOIN analytics.candles_1m kr
      ON kr.bucket = cb.bucket AND kr.symbol = cb.symbol AND kr.exchange = 'kraken'
    WHERE cb.exchange = 'coinbase' AND cb.bucket >= {WINDOW}
    """,
]

HOURLY_SQL = [
    f"DELETE FROM analytics.market_stats_hourly WHERE hour >= {WINDOW}",
    f"""
    WITH c AS (
        SELECT date_trunc('hour', bucket) AS hour, exchange, symbol, bucket,
               open, high, low, close, notional, volume, buy_volume, trades,
               ln(close / lag(close) OVER (PARTITION BY exchange, symbol ORDER BY bucket)) AS r
        FROM analytics.candles_1m
        WHERE bucket >= {WINDOW} - interval '1 minute'
    )
    INSERT INTO analytics.market_stats_hourly
        (hour, exchange, symbol, open, high, low, close, return_pct, volatility_pct,
         notional_usd, buy_share, trades)
    SELECT hour, exchange, symbol,
           (array_agg(open ORDER BY bucket))[1],
           max(high),
           min(low),
           (array_agg(close ORDER BY bucket DESC))[1],
           100 * ((array_agg(close ORDER BY bucket DESC))[1]
                  / (array_agg(open ORDER BY bucket))[1] - 1),
           100 * stddev_samp(r),
           sum(notional),
           sum(buy_volume) / NULLIF(sum(volume), 0),
           sum(trades)
    FROM c
    WHERE hour >= {WINDOW}
    GROUP BY 1, 2, 3
    """,
]

WHALES_SQL = f"""
    INSERT INTO analytics.whale_trades
        (exchange, symbol, trade_id, trade_ts, side, price, size, notional_usd)
    SELECT exchange, symbol, trade_id, trade_ts, side, price, size, notional_usd
    FROM raw.trades
    WHERE trade_ts >= {WINDOW} AND notional_usd >= %(whale)s
    ON CONFLICT DO NOTHING
"""


def hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


@dag(
    dag_id="crypto_pulse_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["streaming", "crypto"],
    doc_md=__doc__,
)
def crypto_pulse_pipeline():
    @task
    def check_freshness() -> dict:
        rows = hook().get_records(
            """
            SELECT exchange, max(trade_ts),
                   count(*) FILTER (WHERE trade_ts > now() - make_interval(mins => %(m)s))
            FROM raw.trades
            WHERE trade_ts > now() - interval '1 day'
            GROUP BY exchange
            """,
            parameters={"m": FRESHNESS_MINUTES},
        )
        recent = {exchange: count for exchange, _, count in rows}
        stale = [e for e in EXCHANGES if not recent.get(e)]
        if stale:
            raise AirflowException(
                f"No trades in the last {FRESHNESS_MINUTES} min from: {stale}. "
                "Check `docker compose logs producer consumer`."
            )
        return recent

    @task
    def build_candles_1m() -> None:
        hook().run(CANDLES_SQL, parameters=PARAMS)

    @task
    def build_spread_1m() -> None:
        hook().run(SPREAD_SQL, parameters=PARAMS)

    @task
    def build_market_stats_hourly() -> None:
        hook().run(HOURLY_SQL, parameters=PARAMS)

    @task
    def capture_whale_trades() -> None:
        hook().run(WHALES_SQL, parameters=PARAMS)

    @task
    def run_data_quality_checks() -> None:
        h = hook()
        checks: list[tuple] = []

        bad_candles = h.get_first(
            f"""
            SELECT count(*) FROM analytics.candles_1m
            WHERE bucket >= {WINDOW}
              AND (low > LEAST(open, close) OR high < GREATEST(open, close) OR low <= 0)
            """,
            parameters=PARAMS,
        )[0]
        checks.append(("candle_ohlc_consistent", bad_candles == 0, bad_candles, 0,
                       "candles where low/high don't bound open/close"))

        future = h.get_first(
            "SELECT count(*) FROM raw.trades WHERE trade_ts > now() + interval '1 minute'"
        )[0]
        checks.append(("no_future_trades", future == 0, future, 0,
                       "trades timestamped more than 1 min in the future"))

        max_spread = h.get_first(
            f"""
            SELECT COALESCE(max(abs(spread_bps)), 0) FROM analytics.spread_1m
            WHERE bucket >= {WINDOW} AND symbol = 'BTC'
            """,
            parameters=PARAMS,
        )[0]
        checks.append(("btc_spread_sane", float(max_spread) <= MAX_SANE_SPREAD_BPS,
                       round(float(max_spread), 2), MAX_SANE_SPREAD_BPS,
                       "largest Coinbase/Kraken BTC gap in bps (huge = likely bad data)"))

        raw_count, candle_count = h.get_first(
            """
            WITH h AS (SELECT date_trunc('hour', now()) - interval '1 hour' AS hour)
            SELECT
              (SELECT count(*) FROM raw.trades, h
                 WHERE trade_ts >= h.hour AND trade_ts < h.hour + interval '1 hour'),
              (SELECT COALESCE(sum(trades), 0) FROM analytics.candles_1m, h
                 WHERE bucket >= h.hour AND bucket < h.hour + interval '1 hour')
            """
        )
        drift = abs(raw_count - candle_count) / max(raw_count, 1)
        checks.append(("candles_reconcile_with_raw", drift <= 0.01, round(drift, 4), 0.01,
                       f"raw={raw_count} vs candles={candle_count} for last complete hour"))

        run_ts = datetime.now(UTC)
        h.insert_rows(
            "analytics.dq_results",
            [(run_ts, *c) for c in checks],
            target_fields=["run_ts", "check_name", "passed", "observed", "threshold", "details"],
        )
        failed = [c[0] for c in checks if not c[1]]
        if failed:
            raise AirflowException(f"Data quality checks failed: {failed}")

    @task
    def prune_raw_trades() -> None:
        hook().run(
            [
                "DELETE FROM raw.trades WHERE trade_ts < now() - make_interval(days => %(d)s)",
                "DELETE FROM analytics.dq_results WHERE run_ts < now() - interval '30 days'",
            ],
            parameters={"d": RETENTION_DAYS},
        )

    fresh = check_freshness()
    candles = build_candles_1m()
    whales = capture_whale_trades()
    spread = build_spread_1m()
    hourly = build_market_stats_hourly()
    dq = run_data_quality_checks()

    fresh >> [candles, whales]
    candles >> [spread, hourly]
    [spread, hourly, whales] >> dq >> prune_raw_trades()


crypto_pulse_pipeline()
