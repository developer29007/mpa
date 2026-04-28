-- MPA pipeline schema.
-- Tables are partitioned by trade_date for efficient range queries and
-- easy lifecycle management (drop old partitions).
--
-- Standard column ordering: trade_date, msg_seq, ts_me, sec_id, exch_id, src_id, ...
--
-- Glossary:
--   msg_seq  — globally unique message sequence number (from publisher)
--   ts_me    — matching engine timestamp, nanoseconds since midnight
--   sec_id   — security identifier (up to 8 chars, e.g. 'AAPL')
--   exch_id  — exchange where the event occurred (e.g. 'XNAS', blank if unknown)
--   src_id   — data source identifier (e.g. 'nasdaq', 'databento', blank if unknown)

CREATE TABLE IF NOT EXISTS trades (
    trade_date      DATE             NOT NULL,
    msg_seq         BIGINT           NOT NULL,
    ts_me           BIGINT           NOT NULL,
    sec_id          VARCHAR(8)       NOT NULL,
    exch_id         VARCHAR(4)       NOT NULL DEFAULT '',
    src_id          VARCHAR(8)       NOT NULL DEFAULT '',
    shares          INTEGER          NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    side            CHAR(1)          NOT NULL,
    trade_type      CHAR(1)          NOT NULL,
    exch_match_id   BIGINT           NOT NULL DEFAULT 0,
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trades_sec_time ON trades (sec_id, ts_me);

CREATE TABLE IF NOT EXISTS vwap (
    trade_date      DATE             NOT NULL,
    msg_seq         BIGINT           NOT NULL,
    ts_me           BIGINT           NOT NULL,
    sec_id          VARCHAR(8)       NOT NULL,
    exch_id         VARCHAR(4)       NOT NULL DEFAULT '',
    src_id          VARCHAR(8)       NOT NULL DEFAULT '',
    interval_ms     INTEGER          NOT NULL,
    vwap_price      DOUBLE PRECISION,
    volume_traded   DOUBLE PRECISION NOT NULL,
    shares_traded   INTEGER          NOT NULL,
    trade_count     INTEGER          NOT NULL,
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_vwap_sec_interval ON vwap (sec_id, interval_ms, ts_me);

CREATE TABLE IF NOT EXISTS tob (
    trade_date              DATE             NOT NULL,
    msg_seq                 BIGINT           NOT NULL,
    ts_me                   BIGINT           NOT NULL,
    sec_id                  VARCHAR(8)       NOT NULL,
    exch_id                 VARCHAR(4)       NOT NULL DEFAULT '',
    src_id                  VARCHAR(8)       NOT NULL DEFAULT '',
    bid_price               DOUBLE PRECISION,
    bid_size                INTEGER          NOT NULL DEFAULT 0,
    ask_price               DOUBLE PRECISION,
    ask_size                INTEGER          NOT NULL DEFAULT 0,
    last_trade_price        DOUBLE PRECISION,
    last_trade_ts_me        BIGINT           NOT NULL DEFAULT 0,
    last_trade_shares       INTEGER          NOT NULL DEFAULT 0,
    last_trade_side         CHAR(1)          NOT NULL DEFAULT ' ',
    last_trade_type         CHAR(1)          NOT NULL DEFAULT ' ',
    last_trade_match_id     BIGINT           NOT NULL DEFAULT 0,
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_tob_sec_time ON tob (sec_id, ts_me);

CREATE TABLE IF NOT EXISTS noii (
    trade_date                DATE             NOT NULL,
    msg_seq                   BIGINT           NOT NULL,
    ts_me                     BIGINT           NOT NULL,
    sec_id                    VARCHAR(8)       NOT NULL,
    exch_id                   VARCHAR(4)       NOT NULL DEFAULT '',
    src_id                    VARCHAR(8)       NOT NULL DEFAULT '',
    paired_shares             BIGINT           NOT NULL,
    imbalance_shares          BIGINT           NOT NULL,
    imbalance_direction       CHAR(1)          NOT NULL,
    far_price                 DOUBLE PRECISION,
    near_price                DOUBLE PRECISION,
    current_reference_price   DOUBLE PRECISION NOT NULL,
    cross_type                CHAR(1)          NOT NULL,
    price_variation_indicator CHAR(1)          NOT NULL,
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_noii_sec_cross ON noii (sec_id, cross_type, ts_me);

-- Per-stock trading halts and resumes (ITCH 'H' Stock Trading Action).
-- event_type: HALT, PAUSE (LULD), QUOTATION (quote-only), RESUME
-- reason: 4-char ITCH code (e.g. 'LUDP', 'MWCB', 'SEC ')
CREATE TABLE IF NOT EXISTS market_events (
    trade_date      DATE             NOT NULL,
    msg_seq         BIGINT           NOT NULL,
    ts_me           BIGINT           NOT NULL,
    sec_id          VARCHAR(8)       NOT NULL DEFAULT '',
    exch_id         VARCHAR(4)       NOT NULL DEFAULT '',
    src_id          VARCHAR(8)       NOT NULL DEFAULT '',
    event_type      VARCHAR(16)      NOT NULL,
    reason          VARCHAR(4)       NOT NULL DEFAULT '',
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_market_events_sec_date
    ON market_events (sec_id, trade_date, ts_me);

-- OHLC + VWAP + aggressor-side breakdown over fixed time intervals.
-- ts_me        — bucket START time (epoch-aligned to interval_ms).
-- notional     — Σ(price × shares), currency-agnostic.
-- buy/sell     — buyer/seller was the aggressor (lifted offer / hit bid).
-- auction      — opening/closing/IPO/halt cross (trade type O, C, H).
-- hidden       — non-displayable execution (midpoint/dark, trade type N).
CREATE TABLE IF NOT EXISTS trade_buckets (
    trade_date      DATE             NOT NULL,
    msg_seq         BIGINT           NOT NULL,
    ts_me           BIGINT           NOT NULL,
    sec_id          VARCHAR(8)       NOT NULL,
    exch_id         VARCHAR(4)       NOT NULL DEFAULT '',
    src_id          VARCHAR(8)       NOT NULL DEFAULT '',
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
    UNIQUE (msg_seq, trade_date)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trade_buckets_sec_interval
    ON trade_buckets (sec_id, interval_ms, ts_me);

-- Helper: convert 'HH:MM:SS' or 'HH:MM:SS.mmm' to nanoseconds since midnight.
CREATE OR REPLACE FUNCTION time_to_ns(t TEXT) RETURNS BIGINT AS $$
    SELECT (EXTRACT(EPOCH FROM t::TIME) * 1000000000)::BIGINT;
$$ LANGUAGE SQL IMMUTABLE;

-- Helper: convert nanoseconds since midnight to 'HH:MM:SS.mmm' (millisecond precision).
CREATE OR REPLACE FUNCTION ns_to_time(ns BIGINT) RETURNS TEXT AS $$
    SELECT TO_CHAR((ns::DOUBLE PRECISION / 1000000000 || ' seconds')::INTERVAL, 'HH24:MI:SS.MS');
$$ LANGUAGE SQL IMMUTABLE;

-- Helper: convert nanoseconds since midnight to 'HH:MM:SS.uuuuuu' (microsecond precision).
CREATE OR REPLACE FUNCTION ns_to_time_us(ns BIGINT) RETURNS TEXT AS $$
    SELECT TO_CHAR((ns::DOUBLE PRECISION / 1000000000 || ' seconds')::INTERVAL, 'HH24:MI:SS.US');
$$ LANGUAGE SQL IMMUTABLE;
