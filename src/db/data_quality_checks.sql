-- Data quality checks for all MPA pipeline tables.
-- Replace '1970-01-01' with the actual trade_date being verified.
-- Every query that returns count(*) or a value should equal the stated expected result.
-- Run via: PYTHONPATH=src pdm run python -c "import os,psycopg; ..."
-- or directly against Postgres with psql.

-- ============================================================
-- TRADES
-- ============================================================

-- [PASS: > 0] Row count
SELECT count(*) AS trades_count FROM trades WHERE trade_date = '1970-01-01';

-- [PASS: multiple types] Distribution by trade type (E=order book, N=non-display, O=open cross, C=close cross, H=IPO/halt)
SELECT trade_type, count(*) AS cnt
FROM trades WHERE trade_date = '1970-01-01'
GROUP BY trade_type ORDER BY trade_type;

-- [PASS: > 0] Distinct stocks traded
SELECT count(DISTINCT sec_id) AS stocks_traded FROM trades WHERE trade_date = '1970-01-01';

-- [PASS: 0] No zero-price or zero-share trades (null cross filter check)
SELECT count(*) AS bad FROM trades
WHERE trade_date = '1970-01-01' AND (price = 0 OR shares = 0);


-- ============================================================
-- TOB (Top of Book)
-- ============================================================

-- [PASS: > 0] Row count
SELECT count(*) AS tob_count FROM tob WHERE trade_date = '1970-01-01';

-- [PASS: > 0] Distinct stocks with TOB snapshots
SELECT count(DISTINCT stock) AS stocks FROM tob WHERE trade_date = '1970-01-01';

-- [PASS: 0] Bid price never above ask price (when both are non-null)
SELECT count(*) AS bad FROM tob
WHERE trade_date = '1970-01-01'
  AND bid_price IS NOT NULL AND ask_price IS NOT NULL
  AND bid_price > ask_price;


-- ============================================================
-- VWAP
-- ============================================================

-- [PASS: all configured intervals present] Rows per interval
SELECT interval_ms, count(*) AS cnt
FROM vwap WHERE trade_date = '1970-01-01'
GROUP BY interval_ms ORDER BY interval_ms;

-- [PASS: > 0] Non-null VWAP count
SELECT count(*) AS cnt FROM vwap
WHERE trade_date = '1970-01-01' AND vwap_price IS NOT NULL;

-- [PASS: 0] No negative VWAP prices
SELECT count(*) AS bad FROM vwap
WHERE trade_date = '1970-01-01' AND vwap_price < 0;


-- ============================================================
-- NOII (Net Order Imbalance Indicator)
-- ============================================================

-- [PASS: > 0] Row count
SELECT count(*) AS noii_count FROM noii WHERE trade_date = '1970-01-01';

-- [PASS: includes O and/or C] Cross type distribution
SELECT cross_type, count(*) AS cnt
FROM noii WHERE trade_date = '1970-01-01'
GROUP BY cross_type ORDER BY cross_type;

-- [PASS: > 0] Reference prices must be positive
SELECT count(*) AS cnt FROM noii
WHERE trade_date = '1970-01-01'
  AND current_reference_price IS NOT NULL
  AND current_reference_price > 0;

-- [PASS: halts ≈ resumes] Imbalance direction distribution
SELECT imbalance_direction, count(*) AS cnt
FROM noii WHERE trade_date = '1970-01-01'
GROUP BY imbalance_direction ORDER BY imbalance_direction;


-- ============================================================
-- MARKET EVENTS
-- ============================================================

-- [PASS: > 0] Row count
SELECT count(*) AS market_events_count FROM market_events WHERE trade_date = '1970-01-01';

-- [PASS: includes HALT and RESUME] Event type breakdown
SELECT event_type, count(*) AS cnt
FROM market_events WHERE trade_date = '1970-01-01'
GROUP BY event_type ORDER BY event_type;

-- [PASS: halts ≈ resumes, within 1-2] Rough pairing check
SELECT
    sum(CASE WHEN event_type IN ('HALT', 'PAUSE') THEN 1 ELSE 0 END) AS halts,
    sum(CASE WHEN event_type = 'RESUME'            THEN 1 ELSE 0 END) AS resumes
FROM market_events WHERE trade_date = '1970-01-01';


-- ============================================================
-- TRADE BUCKETS
-- ============================================================

-- [PASS: > 0] Row count
SELECT count(*) AS trade_buckets_count FROM trade_buckets WHERE trade_date = '1970-01-01';

-- [PASS: all configured intervals present] Rows per interval
SELECT interval_ms, count(*) AS cnt
FROM trade_buckets WHERE trade_date = '1970-01-01'
GROUP BY interval_ms ORDER BY interval_ms;

-- [PASS: 0] No zero open or low — would indicate a null cross slipping through
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01' AND (open = 0 OR low = 0);

-- [PASS: 0] OHLC sanity: low ≤ open ≤ high, low ≤ close ≤ high
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01'
  AND (low > open OR low > close OR high < open OR high < close);

-- [PASS: 0] VWAP must lie within [low, high] — 1e-9 tolerance for float64 rounding
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01'
  AND vwap IS NOT NULL
  AND (vwap < low - 1e-9 OR vwap > high + 1e-9);

-- [PASS: 0] Shares invariant: total_shares = buy + sell + auction + hidden
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01'
  AND total_shares != buy_shares + sell_shares + auction_shares + hidden_shares;

-- [PASS: 0] Volume invariant: notional = sum of all side volumes (1e-6 tolerance)
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01'
  AND abs(notional - (buy_volume + sell_volume + auction_volume + hidden_volume)) > 1e-6;

-- [PASS: 0] No negative share counts
SELECT count(*) AS bad FROM trade_buckets
WHERE trade_date = '1970-01-01'
  AND (total_shares < 0 OR buy_shares < 0 OR sell_shares < 0
       OR auction_shares < 0 OR hidden_shares < 0);

-- [DIAGNOSTIC] Sample rows — inspect for reasonableness
SELECT ns_to_time(timestamp_ns) AS bucket_start, stock, interval_ms,
       round(open::numeric, 3)    AS open,
       round(high::numeric, 3)    AS high,
       round(low::numeric, 3)     AS low,
       round(close::numeric, 3)   AS close,
       round(vwap::numeric, 3)    AS vwap,
       total_shares, buy_shares, sell_shares, auction_shares, hidden_shares,
       trade_count
FROM trade_buckets
WHERE trade_date = '1970-01-01'
ORDER BY interval_ms, stock, timestamp_ns
LIMIT 20;
