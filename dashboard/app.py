"""Crypto Pulse: live trades, cross-exchange spreads and whale alerts."""
from __future__ import annotations

import os
from decimal import Decimal

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://crypto:crypto@localhost:5433/crypto")
DEFAULT_WHALE_USD = int(float(os.getenv("WHALE_USD", "100000")))
UP, DOWN, CB, KR = "#16a34a", "#dc2626", "#2563eb", "#7c3aed"

st.set_page_config(page_title="Crypto Pulse", page_icon="🫀", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def query(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    for col in df.columns:  # Postgres NUMERIC arrives as Decimal; charts want floats
        if df[col].dtype == object and len(df) and isinstance(df[col].iloc[0], Decimal):
            df[col] = df[col].astype(float)
    return df


def utc_naive(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def money(x: float) -> str:
    return f"${x:,.2f}" if x >= 1 else f"${x:,.4f}"


# ---------------------------------------------------------------- sidebar
symbols = query("SELECT DISTINCT symbol FROM raw.trades ORDER BY 1")["symbol"].tolist()

with st.sidebar:
    st.header("Crypto Pulse")
    if symbols:
        default = symbols.index("BTC") if "BTC" in symbols else 0
        symbol = st.selectbox("Asset", symbols, index=default)
    else:
        symbol = None
    exchange = st.radio("Candles from", ["coinbase", "kraken"], horizontal=True)
    window = st.slider("Live window (minutes)", 5, 180, 60, step=5)
    whale_usd = st.number_input("Whale threshold (USD)", 10_000, 10_000_000, DEFAULT_WHALE_USD,
                                step=10_000)
    refresh = st.select_slider("Refresh every (seconds)", [2, 5, 10, 30], value=5)
    st.caption("PAXG is a token backed by physical gold, so it tracks the gold price.")

st.title("🫀 Crypto Pulse")
st.caption("Every trade on Coinbase and Kraken, live. Spot the price gap between exchanges "
           "and catch whale trades as they happen. Times in UTC. Not financial advice.")

if not symbols:
    st.info("Waiting for the first trades. Check the feeds with "
            "`docker compose logs -f producer consumer`.")
    st.stop()

live_tab, pipeline_tab = st.tabs(["Live", "History (Airflow)"])

# ---------------------------------------------------------------- live tab
with live_tab:

    @st.fragment(run_every=refresh)
    def live_view():
        tickers = query(
            """
            WITH t AS (
                SELECT symbol, price,
                       row_number() OVER (PARTITION BY symbol ORDER BY trade_ts DESC) AS newest,
                       row_number() OVER (PARTITION BY symbol ORDER BY trade_ts) AS oldest
                FROM raw.trades
                WHERE trade_ts > now() - make_interval(mins => :m)
            )
            SELECT symbol,
                   max(price) FILTER (WHERE newest = 1) AS last,
                   max(price) FILTER (WHERE oldest = 1) AS first
            FROM t GROUP BY symbol ORDER BY symbol
            """,
            m=window,
        )
        if not tickers.empty:
            for col, row in zip(st.columns(len(tickers)), tickers.itertuples(), strict=True):
                change = (row.last / row.first - 1) * 100 if row.first else 0
                col.metric(row.symbol, money(row.last), f"{change:+.2f}% in {window} min")

        candles = query(
            """
            SELECT date_trunc('minute', trade_ts) AS t,
                   (array_agg(price ORDER BY trade_ts))[1] AS open,
                   max(price) AS high,
                   min(price) AS low,
                   (array_agg(price ORDER BY trade_ts DESC))[1] AS close,
                   sum(notional_usd) FILTER (WHERE side = 'buy') AS buy_usd,
                   sum(notional_usd) FILTER (WHERE side = 'sell') AS sell_usd
            FROM raw.trades
            WHERE symbol = :s AND exchange = :ex
              AND trade_ts > now() - make_interval(mins => :m)
            GROUP BY 1 ORDER BY 1
            """,
            s=symbol, ex=exchange, m=window,
        ).fillna(0)

        st.subheader(f"{symbol} on {exchange.title()}: 1-minute candles and order flow")
        if candles.empty:
            st.write(f"No {symbol} trades on {exchange.title()} in this window.")
        else:
            candles["t"] = utc_naive(candles["t"])
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28],
                                vertical_spacing=0.03)
            fig.add_trace(go.Candlestick(
                x=candles["t"], open=candles["open"], high=candles["high"],
                low=candles["low"], close=candles["close"], name=symbol,
                increasing_line_color=UP, decreasing_line_color=DOWN), row=1, col=1)
            fig.add_trace(go.Bar(x=candles["t"], y=candles["buy_usd"], name="Taker buys",
                                 marker_color=UP), row=2, col=1)
            fig.add_trace(go.Bar(x=candles["t"], y=-candles["sell_usd"], name="Taker sells",
                                 marker_color=DOWN), row=2, col=1)
            fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0), barmode="relative",
                              xaxis_rangeslider_visible=False, legend_orientation="h")
            fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
            fig.update_yaxes(title_text="Flow (USD)", row=2, col=1)
            st.plotly_chart(fig, use_container_width=True)

        left, right = st.columns([3, 2])

        with left:
            st.subheader("Price gap between exchanges")
            gap = query(
                """
                SELECT date_trunc('minute', trade_ts) AS t, exchange,
                       (array_agg(price ORDER BY trade_ts DESC))[1] AS close
                FROM raw.trades
                WHERE symbol = :s AND trade_ts > now() - make_interval(mins => :m)
                GROUP BY 1, 2
                """,
                s=symbol, m=window,
            )
            wide = gap.pivot_table(index="t", columns="exchange", values="close").dropna()
            if {"coinbase", "kraken"} <= set(wide.columns) and not wide.empty:
                wide.index = utc_naive(pd.Series(wide.index)).values
                wide["spread_bps"] = (wide["coinbase"] / wide["kraken"] - 1) * 10_000
                latest = wide["spread_bps"].iloc[-1]
                cheaper = "Kraken" if latest > 0 else "Coinbase"
                a, b = st.columns(2)
                a.metric("Current gap", f"{latest:+.1f} bps", f"{cheaper} is cheaper",
                         delta_color="off")
                b.metric("Widest gap in window", f"{wide['spread_bps'].abs().max():.1f} bps")
                fig = go.Figure(go.Scatter(x=wide.index, y=wide["spread_bps"], mode="lines",
                                           line_color=CB, fill="tozeroy"))
                fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0),
                                  yaxis_title="Coinbase vs Kraken (bps)")
                st.plotly_chart(fig, use_container_width=True)
                st.caption("1 bps = 0.01%. Positive means Coinbase is pricier. Real arbitrage "
                           "also has to beat fees and transfer time.")
            else:
                st.write(f"{symbol} needs trades on both exchanges to compare.")

        with right:
            st.subheader(f"🐋 Whale trades ≥ ${whale_usd:,.0f}")
            whales = query(
                """
                SELECT trade_ts AS time, exchange, symbol, side, price, size,
                       notional_usd AS usd
                FROM raw.trades
                WHERE notional_usd >= :w AND trade_ts > now() - make_interval(mins => :m)
                ORDER BY trade_ts DESC LIMIT 25
                """,
                w=whale_usd, m=window,
            )
            if whales.empty:
                st.write("No whales in this window yet. Try a lower threshold.")
            else:
                whales["side"] = whales["side"].map({"buy": "🟢 buy", "sell": "🔴 sell"})
                st.dataframe(
                    whales, hide_index=True, use_container_width=True, height=330,
                    column_config={
                        "time": st.column_config.DatetimeColumn(format="HH:mm:ss"),
                        "usd": st.column_config.NumberColumn(format="$%.0f"),
                        "price": st.column_config.NumberColumn(format="%.2f"),
                    },
                )

        st.subheader(f"Live tape: latest {symbol} trades")
        tape = query(
            """
            SELECT trade_ts AS time, exchange, side, price, size, notional_usd AS usd
            FROM raw.trades WHERE symbol = :s ORDER BY trade_ts DESC LIMIT 15
            """,
            s=symbol,
        )
        st.dataframe(
            tape, hide_index=True, use_container_width=True,
            column_config={
                "time": st.column_config.DatetimeColumn(format="HH:mm:ss.SSS"),
                "usd": st.column_config.NumberColumn(format="$%.2f"),
            },
        )

    live_view()

# ---------------------------------------------------------------- history tab
with pipeline_tab:
    stats = query(
        """
        SELECT hour, symbol, avg(volatility_pct) AS volatility_pct,
               sum(notional_usd) AS volume_usd, avg(buy_share) AS buy_share
        FROM analytics.market_stats_hourly
        WHERE hour > now() - interval '48 hours'
        GROUP BY 1, 2 ORDER BY 1
        """
    )
    if stats.empty:
        st.info("No history yet. Airflow runs every 5 minutes; open http://localhost:8080 "
                "to watch `crypto_pulse_pipeline`.")
    else:
        stats["hour"] = utc_naive(stats["hour"])
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Hourly volatility (%)")
            st.line_chart(stats.pivot_table(index="hour", columns="symbol",
                                            values="volatility_pct"))
            st.caption("Standard deviation of 1-minute log returns within each hour.")
        with c2:
            st.subheader("Hourly volume (USD)")
            st.bar_chart(stats.pivot_table(index="hour", columns="symbol", values="volume_usd"))

        st.subheader("Exchange gap, last 24 h")
        spread = query(
            """
            SELECT symbol,
                   avg(abs(spread_bps)) AS avg_abs_bps,
                   max(abs(spread_bps)) AS max_abs_bps,
                   100 * avg(CASE WHEN spread_bps > 0 THEN 1.0 ELSE 0.0 END)
                       AS share_coinbase_pricier,
                   count(*) AS minutes
            FROM analytics.spread_1m
            WHERE bucket > now() - interval '24 hours'
            GROUP BY symbol ORDER BY avg_abs_bps DESC
            """
        )
        st.dataframe(
            spread, hide_index=True, use_container_width=True,
            column_config={
                "avg_abs_bps": st.column_config.NumberColumn("Avg gap (bps)", format="%.2f"),
                "max_abs_bps": st.column_config.NumberColumn("Max gap (bps)", format="%.2f"),
                "share_coinbase_pricier": st.column_config.ProgressColumn(
                    "Coinbase pricier", min_value=0, max_value=100, format="%.0f%%"),
            },
        )

        st.subheader("Whale log")
        whale_log = query(
            """
            SELECT date_trunc('hour', trade_ts) AS hour, symbol,
                   count(*) AS whales, sum(notional_usd) AS usd,
                   sum(CASE WHEN side = 'buy' THEN notional_usd ELSE 0 END) AS buy_usd
            FROM analytics.whale_trades
            WHERE trade_ts > now() - interval '48 hours'
            GROUP BY 1, 2 ORDER BY 1 DESC, 4 DESC
            """
        )
        st.dataframe(whale_log, hide_index=True, use_container_width=True,
                     column_config={"usd": st.column_config.NumberColumn(format="$%.0f"),
                                    "buy_usd": st.column_config.NumberColumn(format="$%.0f")})

    st.subheader("Data quality, latest run")
    dq = query(
        """
        SELECT check_name, passed, observed, threshold, details, run_ts
        FROM analytics.dq_results
        WHERE run_ts = (SELECT max(run_ts) FROM analytics.dq_results)
        ORDER BY check_name
        """
    )
    if dq.empty:
        st.write("No data quality runs recorded yet.")
    else:
        st.dataframe(dq, hide_index=True, use_container_width=True)
