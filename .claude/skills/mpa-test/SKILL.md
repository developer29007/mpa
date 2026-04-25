description: Run end-to-end integration tests for a specific MPA pipeline feature (trades | tob | vwap | noii | all). Invoke when asked to test a feature, verify correctness after a code change, or validate the full data flow from ITCH parsing through to Postgres. Works in two modes — simple (--db, no Kafka needed) or full-pipeline (--kafka + db_consumer).
allowed-tools: Bash, Read

# MPA Integration Test Skill

## Pipeline architecture

```
ITCH binary file
    ↓
itch_runner   ──→  [--kafka]  →  Kafka topics  →  db_consumer  →  Postgres
              └──→  [--db]    →  Postgres (direct, no Kafka)
```

Two modes are available:
- **Simple mode** (`--db $MPA_DSN`): itch_runner writes directly to Postgres. One process, no Kafka needed. Preferred for feature testing.
- **Full-pipeline mode** (`--kafka` + `db_consumer`): tests the complete Kafka pipeline. Use when testing the consumer or broker integration.

---

## Connection settings

All commands read connection strings from environment variables. **Before running any step**, load `.env`:

```bash
[ -f .env ] && set -a && source .env && set +a
```

| Variable | Used for |
|----------|----------|
| `MPA_DSN` | Postgres DSN (`postgresql://user:pass@host:5432/mpa`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address (`host:9092`) |

Copy `.env.example` to `.env` and fill in your credentials. The `.env` file is gitignored and must never be committed.

---

## Test isolation

Always use `--date 01011970` (→ `trade_date = 1970-01-01`). This routes all test data to the `*_19700101` Postgres partitions and never touches real production data. Clean up with:

```sql
DELETE FROM noii  WHERE trade_date = '1970-01-01';
DELETE FROM trades WHERE trade_date = '1970-01-01';
DELETE FROM tob   WHERE trade_date = '1970-01-01';
DELETE FROM vwap  WHERE trade_date = '1970-01-01';
```

---

## Step-by-step test workflow

### Step 0 — Load environment

Run this once at the start of every test session before any other step:

```bash
[ -f .env ] && set -a && source .env && set +a
echo "MPA_DSN=${MPA_DSN:?'ERROR: MPA_DSN not set — copy .env.example to .env and fill in credentials'}"
echo "KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
```

If `MPA_DSN` is not set, stop and ask the user to create a `.env` file.

**Python interpreter:** This project uses PDM. Always invoke Python as `pdm run python` (not `python` or `python3`) — the system Python does not have the project's dependencies.

### Step 1 — Find the ITCH binary

```bash
ls data/*.NASDAQ_ITCH50 2>/dev/null | head -3
```

If no file exists, ask the user where the ITCH binary is. Do not proceed without it.

### Step 2 — Pre-flight checks

Check Postgres is reachable:
```bash
PYTHONPATH=src pdm run python -c "
import os, psycopg
conn = psycopg.connect(os.environ['MPA_DSN'], connect_timeout=5)
conn.close()
print('Postgres: OK')
"
```

For full-pipeline mode, also check Kafka:
```bash
PYTHONPATH=src pdm run python -c "
import os
from confluent_kafka.admin import AdminClient
admin = AdminClient({'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'), 'socket.timeout.ms': 5000})
admin.list_topics(timeout=5)
print('Kafka: OK')
"
```

### Step 3 — Clean existing test data

Delete any rows from a prior test run so counts are accurate:
```bash
PYTHONPATH=src pdm run python -c "
import os, psycopg
from psycopg import sql
conn = psycopg.connect(os.environ['MPA_DSN'])
tables = ['trades', 'tob', 'vwap', 'noii']   # adjust to feature tables below
for t in tables:
    with conn.cursor() as cur:
        cur.execute(sql.SQL('DELETE FROM {} WHERE trade_date = %s').format(sql.Identifier(t)), ('1970-01-01',))
        print(f'Deleted {cur.rowcount} rows from {t}')
conn.commit()
conn.close()
"
```

Only delete tables relevant to the feature being tested (see Feature Registry below).

### Step 4a — Run itch_runner (simple mode — preferred)

```bash
PYTHONPATH=src pdm run python -m itch.itch_runner \
    --date 01011970 \
    --file ./data/ITCH_FILE.NASDAQ_ITCH50 \
    --db "$MPA_DSN" \
    --publish FEATURE_PUBLISHERS \
    --max-market-time STOP_TIME
```

Replace `ITCH_FILE` with the actual filename, `FEATURE_PUBLISHERS` with the feature's publish list, and `STOP_TIME` with the market time cutoff (see Feature Registry — each feature specifies a default).

**`--max-market-time` vs `--max-msgs`:** Always use `--max-market-time` for time-bounded tests. It stops on the ITCH message timestamp, not a message count, so it reliably captures all messages up to that wall-clock time regardless of file size or market activity. Never use `--max-msgs` for NOII — message counts vary too much to reliably hit the right window.

**`[db] Done: N <feature>` is the final flush only.** itch_runner flushes to Postgres in batches of 1,000 rows throughout the run; the summary line only shows what was in the buffer at the very end. Always use the SQL count query in Step 5 to get the true total — do not judge success or failure from the Done line alone.

### Step 4b — Run itch_runner + db_consumer (full-pipeline mode)

```bash
# Terminal 1: publish to Kafka (blocks until done)
PYTHONPATH=src pdm run python -m itch.itch_runner \
    --date 01011970 \
    --file ./data/ITCH_FILE.NASDAQ_ITCH50 \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --publish FEATURE_PUBLISHERS \
    --max-market-time STOP_TIME

# Terminal 2 / subprocess with timeout: consume from Kafka into Postgres
PYTHONPATH=src pdm run python -m consumers.db_consumer \
    --date 01011970 \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --dsn "$MPA_DSN" &
CONSUMER_PID=$!
sleep 120        # wait for consumer to drain all messages
kill $CONSUMER_PID 2>/dev/null
wait $CONSUMER_PID 2>/dev/null
echo "Consumer stopped"
```

### Step 5 — Verify Postgres

Run the feature-specific verification queries below. For every `SELECT count(*) AS cnt` query, the result must have `cnt > 0`. For GROUP BY queries, at least one row must be returned.

```bash
PYTHONPATH=src pdm run python -c "
import os, psycopg
conn = psycopg.connect(os.environ['MPA_DSN'])
# paste query here
with conn.cursor() as cur:
    cur.execute('QUERY HERE', {'date': '1970-01-01'})
    for row in cur.fetchall():
        print(row)
conn.close()
"
```

### Step 6 — Report

Print a summary table:

| Check | Result |
|-------|--------|
| .env loaded / MPA_DSN set | PASS / FAIL |
| Postgres health | PASS / FAIL |
| ITCH file exists | PASS / FAIL |
| Test data cleaned | PASS / FAIL |
| itch_runner completed | PASS / FAIL |
| [Feature] row count > 0 | PASS / FAIL |
| [Feature] data quality checks | PASS / FAIL |

State the overall result: **PASS** (all checks passed) or **FAIL** (explain what failed and suggest a fix).

---

## Feature Registry

Use this table to look up `--publish`, tables to clean, and verification SQL for each feature.

---

### `trades`
**Description:** Non-cross trades (ITCH type P) and execution trades (E/C).
**Publish flag:** `--publish trades`
**Tables to clean:** `trades`
**Verification queries:**

```sql
-- Must be > 0
SELECT count(*) AS cnt FROM trades WHERE trade_date = '1970-01-01';

-- Distribution by type (E=execution, N=non-cross, O=open cross, C=close cross)
SELECT trade_type, count(*) AS cnt
FROM trades WHERE trade_date = '1970-01-01'
GROUP BY trade_type ORDER BY trade_type;

-- Must be > 0
SELECT count(DISTINCT sec_id) AS stocks FROM trades WHERE trade_date = '1970-01-01';
```

---

### `tob`
**Description:** Top-of-book snapshots emitted on every order book change.
**Publish flag:** `--publish tob`
**Tables to clean:** `tob`
**Verification queries:**

```sql
-- Must be > 0
SELECT count(*) AS cnt FROM tob WHERE trade_date = '1970-01-01';

-- Must be > 0
SELECT count(DISTINCT stock) AS stocks FROM tob WHERE trade_date = '1970-01-01';
```

---

### `vwap`
**Description:** Rolling VWAP at configurable time intervals (250ms, 1s, 2s, 5s, 10s, 20s).
**Publish flag:** `--publish vwap`
**Tables to clean:** `vwap`
**Verification queries:**

```sql
-- At least one interval bucket must have rows
SELECT interval_ms, count(*) AS cnt
FROM vwap WHERE trade_date = '1970-01-01'
GROUP BY interval_ms ORDER BY interval_ms;

-- Must be > 0
SELECT count(*) AS cnt FROM vwap
WHERE trade_date = '1970-01-01' AND vwap_price IS NOT NULL;
```

---

### `noii`
**Description:** Net Order Imbalance Indicator (ITCH type I). Emitted during opening cross (~9:25–9:30 ET) and closing cross (~3:50–4:00 ET).
**Publish flag:** `--publish noii`
**Default stop time:** `--max-market-time 09:31:00` — captures the full opening cross window and stops early, avoiding an unnecessary full-day scan.
**Full-day test:** omit `--max-market-time` to also capture the closing cross (~3:50–4:00 ET).
**Tables to clean:** `noii`
**Verification queries:**

```sql
-- Must be > 0
SELECT count(*) AS cnt FROM noii WHERE trade_date = '1970-01-01';

-- Expect cross_type O (opening) and/or C (closing)
SELECT cross_type, count(*) AS cnt
FROM noii WHERE trade_date = '1970-01-01'
GROUP BY cross_type ORDER BY cross_type;

-- Imbalance direction distribution
SELECT imbalance_direction, count(*) AS cnt
FROM noii WHERE trade_date = '1970-01-01'
GROUP BY imbalance_direction ORDER BY imbalance_direction;

-- Reference prices must be valid (> 0), must be > 0
SELECT count(*) AS cnt FROM noii
WHERE trade_date = '1970-01-01'
  AND current_reference_price IS NOT NULL
  AND current_reference_price > 0;

-- Sample: first 5 opening-cross messages
SELECT timestamp_ns, stock, imbalance_direction,
       imbalance_shares, current_reference_price, far_price, near_price
FROM noii
WHERE trade_date = '1970-01-01' AND cross_type = 'O'
ORDER BY timestamp_ns LIMIT 5;
```

---

### `market_events`
**Description:** Per-stock trading halts and resumes from ITCH 'H' (Stock Trading Action) messages. Captures `HALT`, `PAUSE` (LULD circuit-breaker), `QUOTATION` (quote-only), and `RESUME` events with their 4-char reason code (e.g. `LUDP`, `MWCB`). Halts occur throughout the trading day; a full-day run is recommended.
**Publish flag:** `--publish market_events`
**Tables to clean:** `market_events`
**Verification queries:**

```sql
-- Must be > 0
SELECT count(*) AS cnt FROM market_events WHERE trade_date = '1970-01-01';

-- Event type distribution (HALT, PAUSE, QUOTATION, RESUME)
SELECT event_type, count(*) AS cnt
FROM market_events WHERE trade_date = '1970-01-01'
GROUP BY event_type ORDER BY event_type;

-- Reason code breakdown
SELECT reason, count(*) AS cnt
FROM market_events WHERE trade_date = '1970-01-01'
GROUP BY reason ORDER BY cnt DESC;

-- Halts and resumes should be roughly paired
SELECT
    sum(CASE WHEN event_type IN ('HALT', 'PAUSE') THEN 1 ELSE 0 END) AS halts,
    sum(CASE WHEN event_type = 'RESUME' THEN 1 ELSE 0 END) AS resumes
FROM market_events WHERE trade_date = '1970-01-01';

-- Sample: first 5 halt events with timestamp
SELECT ns_to_time(timestamp_ns) AS time, stock, event_type, reason
FROM market_events
WHERE trade_date = '1970-01-01' AND event_type IN ('HALT', 'PAUSE')
ORDER BY timestamp_ns LIMIT 5;
```

---

### `candles`
**Description:** Epoch-aligned OHLCV candles with VWAP and bid/offer/auction volume breakdown. Buckets are fixed-boundary (e.g. 09:30:00–09:31:00) at configurable intervals — default 1-min (60 000 ms) and 5-min (300 000 ms). `timestamp_ns` is the bucket START time. `dollar_volume` = Σ(price × shares) enabling cross-bucket aggregation. Use `--max-market-time 09:35:00` for a quick test covering the first few buckets.
**Publish flag:** `--publish candles`
**Default stop time:** `--max-market-time 09:35:00` — captures at least 5 complete 1-min buckets.
**Tables to clean:** `candles`
**Verification queries:**

```sql
-- Must be > 0
SELECT count(*) AS cnt FROM candles WHERE trade_date = '1970-01-01';

-- Both intervals (60000 and 300000 ms) should be present
SELECT interval_ms, count(*) AS cnt
FROM candles WHERE trade_date = '1970-01-01'
GROUP BY interval_ms ORDER BY interval_ms;

-- OHLC sanity: low <= open, close <= high for every candle
SELECT count(*) AS bad FROM candles
WHERE trade_date = '1970-01-01'
  AND (low > open OR low > close OR high < open OR high < close);

-- VWAP sanity: must be within [low, high] (1e-9 tolerance for float64 rounding)
SELECT count(*) AS bad FROM candles
WHERE trade_date = '1970-01-01'
  AND vwap IS NOT NULL
  AND (vwap < low - 1e-9 OR vwap > high + 1e-9);

-- Dollar volume cross-check: 5 consecutive 1-min buckets should equal one 5-min bucket
-- (run manually if needed; just verify vwap is reasonable)
SELECT ns_to_time(timestamp_ns) AS bucket_start, stock, interval_ms,
       open, high, low, close, vwap, total_vol, trade_count
FROM candles
WHERE trade_date = '1970-01-01'
ORDER BY interval_ms, stock, timestamp_ns LIMIT 20;
```

---

### `all`
**Description:** Full pipeline — all six features simultaneously.
**Publish flag:** `--publish all`
**Tables to clean:** `trades`, `tob`, `vwap`, `noii`, `market_events`, `candles`
**Verification queries:** Run all queries from each feature above.

---

## Adding a new feature

When a new feature is added to the pipeline, update this skill by adding a new section to the Feature Registry with:
1. Description
2. `--publish` flag value
3. Default `--max-market-time` cutoff (if applicable)
4. Tables to clean
5. Verification SQL queries
