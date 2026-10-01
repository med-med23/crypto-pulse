CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Every trade from every exchange, written continuously by the consumer
CREATE TABLE IF NOT EXISTS raw.trades (
    exchange      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    trade_id      TEXT NOT NULL,
    price         NUMERIC NOT NULL CHECK (price > 0),
    size          NUMERIC NOT NULL CHECK (size > 0),
    notional_usd  NUMERIC GENERATED ALWAYS AS (price * size) STORED,
    side          TEXT NOT NULL CHECK (side IN ('buy', 'sell')),  -- taker side
    trade_ts      TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (exchange, symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_ts        ON raw.trades (trade_ts);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON raw.trades (symbol, trade_ts);
CREATE INDEX IF NOT EXISTS idx_trades_whales    ON raw.trades (notional_usd) WHERE notional_usd >= 50000;

-- Built by Airflow -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.candles_1m (
    bucket       TIMESTAMPTZ NOT NULL,
    exchange     TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    open         NUMERIC NOT NULL,
    high         NUMERIC NOT NULL,
    low          NUMERIC NOT NULL,
    close        NUMERIC NOT NULL,
    volume       NUMERIC NOT NULL,
    notional     NUMERIC NOT NULL,
    vwap         NUMERIC,
    trades       INT NOT NULL,
    buy_volume   NUMERIC NOT NULL,
    sell_volume  NUMERIC NOT NULL,
    PRIMARY KEY (bucket, exchange, symbol)
);

CREATE TABLE IF NOT EXISTS analytics.spread_1m (
    bucket          TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,
    coinbase_price  NUMERIC NOT NULL,
    kraken_price    NUMERIC NOT NULL,
    spread_bps      NUMERIC NOT NULL,  -- (coinbase - kraken) / kraken * 10,000
    PRIMARY KEY (bucket, symbol)
);

CREATE TABLE IF NOT EXISTS analytics.market_stats_hourly (
    hour            TIMESTAMPTZ NOT NULL,
    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    open            NUMERIC NOT NULL,
    high            NUMERIC NOT NULL,
    low             NUMERIC NOT NULL,
    close           NUMERIC NOT NULL,
    return_pct      NUMERIC,
    volatility_pct  NUMERIC,  -- std dev of 1-minute log returns, in %
    notional_usd    NUMERIC NOT NULL,
    buy_share       NUMERIC,  -- taker-buy volume / total volume
    trades          INT NOT NULL,
    PRIMARY KEY (hour, exchange, symbol)
);

CREATE TABLE IF NOT EXISTS analytics.whale_trades (
    exchange      TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    trade_id      TEXT NOT NULL,
    trade_ts      TIMESTAMPTZ NOT NULL,
    side          TEXT NOT NULL,
    price         NUMERIC NOT NULL,
    size          NUMERIC NOT NULL,
    notional_usd  NUMERIC NOT NULL,
    PRIMARY KEY (exchange, symbol, trade_id)
);

CREATE TABLE IF NOT EXISTS analytics.dq_results (
    run_ts      TIMESTAMPTZ NOT NULL,
    check_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    observed    NUMERIC,
    threshold   NUMERIC,
    details     TEXT,
    PRIMARY KEY (run_ts, check_name)
);
