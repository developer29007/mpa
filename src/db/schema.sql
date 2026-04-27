-- Kafka-to-Postgres schema for MPA analytics data.
-- Tables are partitioned by trade_date for efficient time-range queries
-- and easy data lifecycle management (drop old partitions).
-- msg_id is the publisher-assigned unique identifier used for dedup on crash recovery.

CREATE TABLE IF NOT EXISTS trades (
    trade_date       DATE             NOT NULL,
    msg_id           BIGINT           NOT NULL,
    timestamp_ns     BIGINT           NOT NULL,
    sec_id           VARCHAR(8)       NOT NULL,
    shares           INTEGER          NOT NULL,
    price            DOUBLE PRECISION NOT NULL,
    side             CHAR(1)          NOT NULL,
    trade_type       CHAR(1)          NOT NULL,
    exch_id          VARCHAR(4)       NOT NULL DEFAULT '',
    src              VARCHAR(8)       NOT NULL DEFAULT '',
    exch_match_id    BIGINT           NOT NULL DEFAULT 0,
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trades_sec_time ON trades (sec_id, timestamp_ns);

CREATE TABLE IF NOT EXISTS vwap (
    trade_date       DATE             NOT NULL,
    msg_id           BIGINT           NOT NULL,
    timestamp_ns     BIGINT           NOT NULL,
    stock            VARCHAR(8)       NOT NULL,
    interval_ms      INTEGER          NOT NULL,
    vwap_price       DOUBLE PRECISION,
    volume_traded    DOUBLE PRECISION NOT NULL,
    shares_traded    INTEGER          NOT NULL,
    trade_count      INTEGER          NOT NULL,
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_vwap_stock_interval ON vwap (stock, interval_ms, timestamp_ns);

CREATE TABLE IF NOT EXISTS tob (
    trade_date              DATE             NOT NULL,
    msg_id                  BIGINT           NOT NULL,
    timestamp_ns            BIGINT           NOT NULL,
    stock                   VARCHAR(8)       NOT NULL,
    bid_price               DOUBLE PRECISION,
    bid_size                INTEGER          NOT NULL DEFAULT 0,
    ask_price               DOUBLE PRECISION,
    ask_size                INTEGER          NOT NULL DEFAULT 0,
    last_trade_price        DOUBLE PRECISION,
    last_trade_timestamp_ns BIGINT           NOT NULL DEFAULT 0,
    last_trade_shares       INTEGER          NOT NULL DEFAULT 0,
    last_trade_side         CHAR(1)          NOT NULL DEFAULT ' ',
    last_trade_type         CHAR(1)          NOT NULL DEFAULT ' ',
    last_trade_match_id     BIGINT           NOT NULL DEFAULT 0,
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_tob_stock_time ON tob (stock, timestamp_ns);

CREATE TABLE IF NOT EXISTS noii (
    trade_date                DATE             NOT NULL,
    msg_id                    BIGINT           NOT NULL,
    timestamp_ns              BIGINT           NOT NULL,
    stock                     VARCHAR(8)       NOT NULL,
    paired_shares             BIGINT           NOT NULL,
    imbalance_shares          BIGINT           NOT NULL,
    imbalance_direction       CHAR(1)          NOT NULL,
    far_price                 DOUBLE PRECISION,
    near_price                DOUBLE PRECISION,
    current_reference_price   DOUBLE PRECISION NOT NULL,
    cross_type                CHAR(1)          NOT NULL,
    price_variation_indicator CHAR(1)          NOT NULL,
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_noii_stock_cross ON noii (stock, cross_type, timestamp_ns);

-- Market events: per-stock trading halts and resumes from ITCH 'H' Stock Trading Action.
-- event_type: HALT, PAUSE (LULD), QUOTATION (quote-only), RESUME
-- reason: 4-char ITCH reason code (e.g. 'LUDP', 'MWCB', 'SEC ')
-- Note: opening/closing auction prices are stored in the trades table (trade_type='O'/'C').
CREATE TABLE IF NOT EXISTS market_events (
    trade_date      DATE             NOT NULL,
    msg_id          BIGINT           NOT NULL,
    timestamp_ns    BIGINT           NOT NULL,
    event_type      VARCHAR(16)      NOT NULL,
    stock           VARCHAR(8)       NOT NULL DEFAULT '',
    reason          VARCHAR(4)       NOT NULL DEFAULT '',
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_market_events_stock_date
    ON market_events (stock, trade_date, timestamp_ns);

-- Trade buckets: OHLC + VWAP + notional and share counts broken down by aggressor side.
-- timestamp_ns   — bucket START time (epoch-aligned to interval_ms).
-- notional       — Σ(price × shares) across all trades; currency-agnostic value traded.
-- buy/sell/auction/hidden — aggressor-side breakdown by shares and notional.
--   buy     : buyer was the aggressor (lifted the offer)
--   sell    : seller was the aggressor (hit the bid)
--   auction : opening/closing/IPO/halt cross (trade type O, C, H)
--   hidden  : non-displayable execution (midpoint/dark, trade type N)
CREATE TABLE IF NOT EXISTS trade_buckets (
    trade_date      DATE             NOT NULL,
    msg_id          BIGINT           NOT NULL,
    timestamp_ns    BIGINT           NOT NULL,
    stock           VARCHAR(8)       NOT NULL,
    interval_ms     INTEGER          NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    notional        DOUBLE PRECISION NOT NULL,
    vwap            DOUBLE PRECISION,
    total_shares    INTEGER          NOT NULL,
    buy_shares      INTEGER          NOT NULL,
    sell_shares     INTEGER          NOT NULL,
    auction_shares  INTEGER          NOT NULL,
    hidden_shares   INTEGER          NOT NULL,
    trade_count     INTEGER          NOT NULL,
    buy_volume      DOUBLE PRECISION NOT NULL,
    sell_volume     DOUBLE PRECISION NOT NULL,
    auction_volume  DOUBLE PRECISION NOT NULL,
    hidden_volume   DOUBLE PRECISION NOT NULL,
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trade_buckets_stock_interval
    ON trade_buckets (stock, interval_ms, timestamp_ns);

-- Helper: convert 'HH:MM:SS' or 'HH:MM:SS.mmm' to nanoseconds since midnight.
-- Usage: time_to_ns('10:32:00') → 37920000000000
CREATE OR REPLACE FUNCTION time_to_ns(t TEXT) RETURNS BIGINT AS $$
    SELECT (EXTRACT(EPOCH FROM t::TIME) * 1000000000)::BIGINT;
$$ LANGUAGE SQL IMMUTABLE;

-- Helper: convert nanoseconds since midnight to 'HH:MM:SS.mmm' text (millisecond precision).
-- Usage: ns_to_time(37920123456789) → '10:32:00.123'
CREATE OR REPLACE FUNCTION ns_to_time(ns BIGINT) RETURNS TEXT AS $$
    SELECT TO_CHAR((ns::DOUBLE PRECISION / 1000000000 || ' seconds')::INTERVAL, 'HH24:MI:SS.MS');
$$ LANGUAGE SQL IMMUTABLE;

-- Helper: convert nanoseconds since midnight to 'HH:MM:SS.uuuuuu' text (microsecond precision).
-- Usage: ns_to_time_us(37920123456789) → '10:32:00.123456'
CREATE OR REPLACE FUNCTION ns_to_time_us(ns BIGINT) RETURNS TEXT AS $$
    SELECT TO_CHAR((ns::DOUBLE PRECISION / 1000000000 || ' seconds')::INTERVAL, 'HH24:MI:SS.US');
$$ LANGUAGE SQL IMMUTABLE;
