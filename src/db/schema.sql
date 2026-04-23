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

-- Market events: opening/closing auction prices and per-stock halts/resumes.
-- Sources:
--   ITCH 'Q' Cross Trade    → OPEN_CROSS, CLOSE_CROSS, IPO_CROSS, INTRADAY_CROSS
--   ITCH 'H' Trading Action → HALT, PAUSE, QUOTATION, RESUME
-- price/shares are populated for cross events; NULL/0 for halt events.
-- reason is populated for halt events (e.g. 'LUDP', 'MWCB'); empty for crosses.
CREATE TABLE IF NOT EXISTS market_events (
    trade_date      DATE             NOT NULL,
    msg_id          BIGINT           NOT NULL,
    timestamp_ns    BIGINT           NOT NULL,
    event_type      VARCHAR(16)      NOT NULL,
    stock           VARCHAR(8)       NOT NULL DEFAULT '',
    price           DOUBLE PRECISION,           -- auction cross price; NULL for halt/resume events
    shares          BIGINT           NOT NULL DEFAULT 0,
    reason          VARCHAR(4)       NOT NULL DEFAULT '',
    UNIQUE (msg_id, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_market_events_stock_date
    ON market_events (stock, trade_date, timestamp_ns);

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
