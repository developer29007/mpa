"""
MPA Integration Test Agent
===========================
A Claude-powered agent that runs an end-to-end integration test for one or more
MPA pipeline features: ITCH file → Kafka → Postgres.

The agent understands the full pipeline, knows what each ITCH runner flag produces,
and uses a test isolation date so test data never mixes with production data.

Usage
-----
    python -m agents.test_agent \\
        --feature noii \\
        --itch-file ./data/04012024.NASDAQ_ITCH50 \\
        --date 04012024 \\
        --test-date 01011970 \\
        --kafka localhost:9092 \\
        --dsn postgresql://mpa:mpa@localhost:5432/mpa

Arguments
---------
  --feature      Feature to test: trades | tob | vwap | noii | all
  --itch-file    Path to the ITCH binary file (required)
  --date         Actual data date in MMDDYYYY format (used only for logging/context)
  --test-date    Isolation date in MMDDYYYY format [default: 01011970]
                 All data is published and stored under this date so it never
                 conflicts with real production data.
  --kafka        Kafka bootstrap servers [default: localhost:9092]
  --dsn          Postgres DSN [default: postgresql://mpa:mpa@localhost:5432/mpa]
  --stocks       Optional space-separated stock filter, e.g. AAPL MSFT
  --max-msgs     Limit ITCH messages processed (0=all). WARNING: for noii/all,
                 leave at 0 — NOII messages only appear near market open/close.
  --consumer-timeout
                 Seconds to let db_consumer run before stopping [default: 180]
  --model        Claude model to use [default: claude-opus-4-5]
"""

import argparse
import json
import sys
from datetime import datetime

import anthropic

from agents import agent_tools
from agents.feature_registry import FEATURES

DEFAULT_TEST_DATE = "01011970"
DEFAULT_KAFKA = "localhost:9092"
DEFAULT_DSN = "postgresql://mpa:mpa@localhost:5432/mpa"
DEFAULT_MODEL = "claude-opus-4-5"

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an integration test agent for the MPA (Market Participant Analytics) system — \
a NASDAQ ITCH 5.0 market data pipeline.

## Pipeline Architecture

  ITCH binary file
       ↓
  itch_runner  (Python process — parses ITCH binary, publishes to Kafka)
       ↓  publishes to Kafka topics:  trades | tob | vwap | noii
  db_consumer  (Python process — reads Kafka, writes to Postgres)
       ↓  inserts into Postgres tables (partitioned by trade_date):
  trades | tob | vwap | noii

## What each itch_runner flag enables

| Flag            | Effect                                                        |
|-----------------|---------------------------------------------------------------|
| --kafka <srv>   | Enables ALL four Kafka publishers: trades, tob, vwap, noii   |
| --stocks A B    | Filter: only process those stock symbols (faster tests)       |
| --max-msgs N    | Stop after N total ITCH messages (0 = entire file)            |
| --date MMDDYYYY | Sets trade_date on all published messages; also controls      |
|                 | which Kafka topics are deleted at startup                     |
| --file <path>   | Override the default ITCH file path (./data/{date}.ITCH50)   |

## Test isolation strategy

A "test isolation date" (e.g. 01011970 → 1970-01-01) is used as --date for BOTH
itch_runner and db_consumer. This causes:
- All Kafka messages to carry trade_date = 1970-01-01
- Postgres to partition data under the 1970-01-01 partition
- Zero risk of contaminating production data

## Your testing workflow — follow this order strictly

### Step 1 — Pre-flight checks
1. check_kafka_health → must be healthy before proceeding
2. check_postgres_health → must be healthy before proceeding
3. check_itch_file → file must exist; report size
4. If any check fails, report FAIL and stop.

### Step 2 — Clean up and verify schema
- Call cleanup_test_data for all tables in this feature.
- For each table, call get_table_schema to confirm the table exists (exists=true).
  If a table does not exist, report FAIL immediately — the schema migration is missing.

### Step 3 — Run itch_runner
- Use the test_date as --date, and --file pointing to the real ITCH data file.
- itch_runner deletes and recreates all Kafka topics at startup (clean slate).
- Report how many messages were processed (from stdout).

### Step 4 — Run db_consumer
- Use the same test_date as --date.
- Let it run until idle then stop via SIGTERM (handled by timeout_seconds).
- Report how many rows were flushed to Postgres (from stdout).

### Step 5 — Verify Kafka message counts
- For each topic in this feature, call get_kafka_message_count.
- If a topic has 0 messages, that is a failure for that feature.

### Step 6 — Verify Postgres data
- Run each verify_query from the feature registry using run_sql.
- Use trade_date = the ISO form of test_date as the SQL parameter.
- For each query apply TWO checks:
  1. row_count < min_rows → FAIL (not enough groups or distinct values returned).
  2. If min_count is present: rows[0][0] < min_count → FAIL (the scalar count value is
     too low; the table may be empty even though the query returned 1 row).

### Step 7 — Report
- Print a summary table: each check → PASS / FAIL / INFO.
- State the overall result: PASS (all checks passed) or FAIL (at least one failed).
- For failures, explain exactly what was missing or wrong and suggest a fix.

## Important NOII notes
- NOII messages (ITCH type 'I') only appear during NASDAQ crossing sessions:
  Opening cross: disseminated every 10s from 9:25 AM, then every 1s from 9:28 AM ET.
  Closing cross: disseminated every 10s from 3:50 PM, then every 1s from 3:55 PM ET.
- If --max-msgs is too small, the parser may not reach those windows and NOII count = 0.
- For NOII testing, always use max_msgs=0 (process the full file) or a large enough value.
- A typical full-day NASDAQ ITCH file is 3–15 GB and contains 200–500M messages.

## Important timing note
- itch_runner is a batch job that exits when done.
- db_consumer is long-running; timeout_seconds controls when it is stopped.
- Always run itch_runner FIRST, wait for it to finish, THEN run db_consumer.
"""

# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

_TOOL_FN_MAP = {
    "check_kafka_health":      agent_tools.check_kafka_health,
    "check_postgres_health":   agent_tools.check_postgres_health,
    "check_itch_file":         agent_tools.check_itch_file,
    "get_kafka_message_count": agent_tools.get_kafka_message_count,
    "get_table_schema":        agent_tools.get_table_schema,
    "run_itch_runner":         agent_tools.run_itch_runner,
    "run_db_consumer":         agent_tools.run_db_consumer,
    "run_sql":                 agent_tools.run_sql,
    "cleanup_test_data":       agent_tools.cleanup_test_data,
}


def _dispatch_tool(name: str, tool_input: dict) -> dict:
    fn = _TOOL_FN_MAP.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    try:
        return fn(**tool_input)
    except Exception as e:
        return {"error": f"Tool '{name}' raised {type(e).__name__}: {e}"}


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def run_agent(config: dict) -> int:
    """Run the test agent. Returns 0 on PASS, 1 on FAIL."""
    feature_key = config["feature"]
    feature = FEATURES[feature_key]

    test_date_obj = datetime.strptime(config["test_date"], "%m%d%Y").date()
    test_date_iso = test_date_obj.isoformat()

    stocks_str = " ".join(config["stocks"]) if config.get("stocks") else "all stocks"
    max_msgs_str = str(config["max_msgs"]) if config.get("max_msgs") else "unlimited (full file)"

    user_message = f"""\
Run a full integration test for the **{feature_key}** feature.

## Test configuration
| Parameter         | Value                                         |
|-------------------|-----------------------------------------------|
| Feature           | {feature_key} — {feature['description']}     |
| ITCH file         | {config['itch_file']}                         |
| Data date         | {config['date']} (for context only)           |
| Test isolation date | {config['test_date']} → trade_date={test_date_iso} |
| Kafka             | {config['kafka']}                             |
| Postgres DSN      | {config['dsn']}                               |
| Stock filter      | {stocks_str}                                  |
| Max messages      | {max_msgs_str}                                |
| Consumer timeout  | {config['consumer_timeout']}s                 |

## Expected Kafka topics
{feature['topics']}

## Expected Postgres tables
{feature['tables']}

## Verification queries to run
{json.dumps(feature['verify_queries'], indent=2)}

Please proceed with the full integration test now.
"""

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": user_message}]

    print(f"\n{'='*60}")
    print(f"  MPA Integration Test Agent")
    print(f"  Feature: {feature_key}  |  Test date: {test_date_iso}")
    print(f"{'='*60}\n")

    while True:
        response = client.messages.create(
            model=config.get("model", DEFAULT_MODEL),
            max_tokens=8192,
            system=SYSTEM_PROMPT,
            tools=agent_tools.TOOL_DEFINITIONS,
            messages=messages,
        )

        # Print any text the agent produces
        for block in response.content:
            if hasattr(block, "text") and block.text:
                print(block.text)

        if response.stop_reason == "end_turn":
            # Heuristic: scan last assistant text for PASS/FAIL verdict
            final_text = " ".join(
                b.text for b in response.content if hasattr(b, "text")
            ).upper()
            if "OVERALL RESULT: FAIL" in final_text or "RESULT: FAIL" in final_text:
                return 1
            return 0

        if response.stop_reason != "tool_use":
            print(f"\n[agent] Unexpected stop_reason: {response.stop_reason}", file=sys.stderr)
            return 1

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            print(f"\n[tool] {block.name}({json.dumps(block.input, default=str)})")
            result = _dispatch_tool(block.name, block.input)
            result_str = json.dumps(result, default=str)
            # Print a short preview so the human can follow along
            preview = result_str[:300] + "..." if len(result_str) > 300 else result_str
            print(f"[tool] → {preview}")
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": result_str,
            })

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="MPA integration test agent — drives Claude to test the full pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--feature",
        required=True,
        choices=list(FEATURES.keys()),
        help="Feature to test",
    )
    parser.add_argument(
        "--itch-file",
        required=True,
        help="Path to the ITCH binary file, e.g. ./data/04012024.NASDAQ_ITCH50",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Actual data date in MMDDYYYY format (for context/logging only)",
    )
    parser.add_argument(
        "--test-date",
        default=DEFAULT_TEST_DATE,
        help=f"Test isolation date in MMDDYYYY [default: {DEFAULT_TEST_DATE} → 1970-01-01]",
    )
    parser.add_argument("--kafka", default=DEFAULT_KAFKA, help="Kafka bootstrap servers")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="Postgres DSN")
    parser.add_argument(
        "--stocks",
        nargs="+",
        default=None,
        metavar="SYMBOL",
        help="Only process these stock symbols (optional, speeds up tests)",
    )
    parser.add_argument(
        "--max-msgs",
        type=int,
        default=0,
        help="Stop after N ITCH messages (0=entire file). Avoid for noii/all features.",
    )
    parser.add_argument(
        "--consumer-timeout",
        type=int,
        default=180,
        help="Seconds to let db_consumer run before SIGTERM [default: 180]",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Claude model [default: {DEFAULT_MODEL}]",
    )
    args = parser.parse_args()

    config = {
        "feature": args.feature,
        "itch_file": args.itch_file,
        "date": args.date,
        "test_date": args.test_date,
        "kafka": args.kafka,
        "dsn": args.dsn,
        "stocks": args.stocks,
        "max_msgs": args.max_msgs,
        "consumer_timeout": args.consumer_timeout,
        "model": args.model,
    }

    return run_agent(config)


if __name__ == "__main__":
    sys.exit(main())
