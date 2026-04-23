# MPA — Market Participant Analytics

## What this project does

MPA is a NASDAQ ITCH 5.0 market data pipeline:

```
ITCH binary file
    ↓
itch_runner   — parses binary, publishes messages to Kafka
    ↓  topics: trades | tob | vwap | noii
db_consumer   — reads Kafka, inserts into Postgres (partitioned by trade_date)
    ↓  tables: trades | tob | vwap | noii
FastAPI       — REST query layer on top of Postgres
```

## Prerequisites

All services run via Docker Compose. Start them with:

```bash
docker-compose up -d kafka postgres
# wait ~10s for Kafka to be ready, then:
docker-compose up -d api db-consumer
```

Or run services manually (see below).

---

## ITCH Runner reference

**Command:**
```bash
PYTHONPATH=src python -m itch.itch_runner --date MMDDYYYY [OPTIONS]
```

**All flags:**

| Flag | Default | Meaning |
|---|---|---|
| `--date MMDDYYYY` | required | Business date. Sets `trade_date` on all messages. Also controls which Kafka topics are deleted at startup. |
| `--file PATH` | `./data/{date}.NASDAQ_ITCH50` | Override the ITCH binary file path. |
| `--kafka HOST:PORT` | None | Enable Kafka publishing. When set, ALL four topics are published: `trades`, `tob`, `vwap`, `noii`. Without this flag, nothing is published. |
| `--stocks A B …` | All stocks | Filter: only process these symbols. Greatly speeds up tests. |
| `--max-msgs N` | 0 (all) | Stop after N total ITCH messages. Use for quick smoke tests. **Do not use for NOII testing** — message counts vary; use `--max-market-time` instead. |
| `--max-market-time HH:MM:SS` | None | Stop when ITCH timestamp reaches this wall-clock time. Preferred over `--max-msgs` for time-bounded tests. Example: `09:31:00` captures the full opening cross without scanning the whole day. |
| `--publish A B …` | `all` | Kafka publishers to enable: `trades` `tob` `vwap` `noii` `all`. Only the selected topics are deleted at startup. Useful for targeted tests (e.g. `--publish noii` to only write NOII data). |
| `--bucket-intervals MS …` | 250 1000 2000 5000 10000 20000 | VWAP rolling window sizes in milliseconds. |
| `--print-trades STOCK …` | None | Print trade executions to stdout for the given stocks. |
| `--print-vwap STOCK …` | None | Print VWAP updates to stdout. |
| `--chart [STOCK …]` | None | Start a WebSocket trade chart server (port 8765). |
| `--candle-chart [STOCK …]` | None | Start a WebSocket candle chart server (port 8766). |

**What `--kafka` enables:**

| Kafka topic | ITCH message types consumed | Published when |
|---|---|---|
| `trades` | P (non-cross), E (order executed), C (executed w/ price), Q (cross trade) | Any trade execution |
| `tob` | A/F/E/C/X/D/U (all order book changes) | Every order book state change |
| `vwap` | Same as trades | Timer-based (every `--bucket-intervals` ms) |
| `noii` | I (Net Order Imbalance Indicator) | Opening cross (~9:25–9:30 ET), Closing cross (~3:50–4:00 ET) |

**When itch_runner starts with `--kafka`, it deletes all four topics first for a clean slate.**

---

## db_consumer reference

**Command:**
```bash
PYTHONPATH=src python -m consumers.db_consumer \
    --date MMDDYYYY \
    --kafka HOST:PORT \
    --dsn postgresql://user:pass@host:5432/mpa \
    [--batch-size 1000] \
    [--flush-interval 2.0]
```

| Flag | Default | Meaning |
|---|---|---|
| `--date MMDDYYYY` | required | All inserted rows get this `trade_date`. Creates the daily partition if it doesn't exist. |
| `--kafka HOST:PORT` | required | Kafka bootstrap servers. |
| `--dsn DSN` | required | Postgres connection string. |
| `--batch-size N` | 1000 | Number of Kafka messages to buffer before flushing to Postgres. |
| `--flush-interval S` | 2.0 | Max seconds between Postgres flushes (even if batch-size not reached). |

The consumer subscribes to all four topics: `trades`, `tob`, `vwap`, `noii`.
It handles SIGTERM gracefully: performs a final flush then exits cleanly.

---

## NOII — Net Order Imbalance Indicator

NOII messages (ITCH type `I`) carry pre-cross imbalance data that predicts NASDAQ auction prices.

**NOII fields:**

| Field | Type | Values |
|---|---|---|
| `paired_shares` | uint64 | Total shares matchable at current reference price |
| `imbalance_shares` | uint64 | Shares not paired (excess on one side) |
| `imbalance_direction` | char | `B`=buy, `S`=sell, `N`=none, `O`=insufficient |
| `stock` | str | Symbol |
| `far_price` | float\|NULL | Cross price using only cross orders (NULL if N/A) |
| `near_price` | float\|NULL | Cross price using cross+continuous orders (NULL if N/A) |
| `current_reference_price` | float | Price NASDAQ uses to conduct the cross |
| `cross_type` | char | `O`=opening, `C`=closing, `H`=IPO/halt, `A`=extended trading close |
| `price_variation_indicator` | char | `L`=<1%, `1`–`9`=1%–9.99%, `A`=10%–19.99%, `B`=20%–29.99%, `C`=≥30%, ` `=N/A |

**NOII timing (ET):**

| Cross | Dissemination starts | Frequency |
|---|---|---|
| Opening | 9:25 AM | Every 10s → every 1s from 9:28 AM |
| Closing | 3:50 PM | Every 10s → every 1s from 3:55 PM |
| IPO/Halt | 1s after quoting period opens | Every 1s |

**Signal research use cases:**
- Compare `current_reference_price` vs first print (opening trade) — gap = predictive alpha
- Track `imbalance_direction` and `imbalance_shares` build-up → ORB (Opening Range Breakout) signal
- Closing NOII `far_price` vs prior day close — overnight gap predictor

---

## Test isolation

All test runs use a "test isolation date" (default: `01011970` → `1970-01-01`) as `--date`
for both `itch_runner` and `db_consumer`. This:

1. Puts all test data in the `*_19700101` Postgres partitions.
2. Never contaminates production data for the actual market date.
3. Makes cleanup trivial: `DELETE FROM noii WHERE trade_date = '1970-01-01'`.

The actual ITCH binary file is passed via `--file` so the parser reads real market data.

---

## Integration testing

Use the `mpa-test` Claude Code skill to run integration tests for any feature:

```bash
/mpa-test noii      # test NOII (morning cross only, stops at 09:31)
/mpa-test trades    # test trades
/mpa-test all       # test full pipeline
```

The skill handles environment loading, pre-flight checks, cleanup, running itch_runner in `--db` mode (no Kafka needed), and SQL verification. See `.claude/skills/mpa-test/SKILL.md` for the full workflow and feature registry.

---

## Manual verification SQL

After running the pipeline, use these queries (replace `'1970-01-01'` with your test date):

```sql
-- NOII: count by cross type
SELECT cross_type, count(*) FROM noii
WHERE trade_date = '1970-01-01' GROUP BY cross_type;

-- NOII: first 5 opening-cross messages
SELECT ns_to_time(timestamp_ns), stock, imbalance_direction,
       imbalance_shares, current_reference_price, far_price, near_price
FROM noii
WHERE trade_date = '1970-01-01' AND cross_type = 'O'
ORDER BY timestamp_ns LIMIT 5;

-- Trades: count by type
SELECT trade_type, count(*) FROM trades
WHERE trade_date = '1970-01-01' GROUP BY trade_type;

-- VWAP: latest VWAP per stock at 1000ms interval
SELECT stock, vwap_price, shares_traded
FROM vwap
WHERE trade_date = '1970-01-01' AND interval_ms = 1000
ORDER BY timestamp_ns DESC LIMIT 20;

-- TOB: latest snapshot per stock
SELECT DISTINCT ON (stock)
    stock, bid_price, bid_size, ask_price, ask_size
FROM tob
WHERE trade_date = '1970-01-01'
ORDER BY stock, timestamp_ns DESC;

-- Clean up test data
DELETE FROM trades WHERE trade_date = '1970-01-01';
DELETE FROM tob    WHERE trade_date = '1970-01-01';
DELETE FROM vwap   WHERE trade_date = '1970-01-01';
DELETE FROM noii   WHERE trade_date = '1970-01-01';
```

---

## Project layout

```
src/
├── itch/            # ITCH 5.0 parser, feed handler, CLI runners
├── book/            # Order book, trade/TOB/NOII models and listener interfaces
├── publishers/      # Kafka publishers (trade, tob, vwap, noii)
├── consumers/       # Kafka-to-Postgres consumer and deserializers
├── db/              # Postgres connection, inserter, schema.sql
├── analytics/       # VWAP/TWAP calculation engines
├── api/             # FastAPI REST service
└── util/            # Time utilities, timer service, message ID generator
```

---

## Development workflow

```bash
# Install deps
pdm install

# Run tests
pdm run pytest

# Lint / type check
pdm run mypy src/

# Load credentials (copy .env.example → .env and fill in your values)
source .env

# Run the ITCH runner locally (requires ITCH file in ./data/)
PYTHONPATH=src python -m itch.itch_runner \
    --date 04012024 \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --stocks AAPL MSFT NVDA \
    --max-msgs 5000000

# Run db_consumer locally
PYTHONPATH=src python -m consumers.db_consumer \
    --date 04012024 \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --dsn "$MPA_DSN"
```
