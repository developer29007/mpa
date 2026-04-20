"""
Concrete tool implementations for the MPA integration test agent.

Each function is called by the Claude tool-use loop in test_agent.py.
All subprocess commands set PYTHONPATH so modules in src/ are importable.
"""

import os
import signal as _signal
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# src/ directory — always add to subprocess env so modules resolve correctly
_SRC_DIR = str(Path(__file__).parent.parent)


def _env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = _SRC_DIR
    return env


def _trunc(text: str | None, chars: int = 4000) -> str:
    if not text:
        return ""
    return text[-chars:] if len(text) > chars else text


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def check_kafka_health(bootstrap_servers: str) -> dict[str, Any]:
    """Try connecting to Kafka and listing topics. Returns healthy=True/False."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": 5000,
            "request.timeout.ms": 5000,
        })
        metadata = admin.list_topics(timeout=5)
        user_topics = [t for t in metadata.topics if not t.startswith("_")]
        return {"healthy": True, "topic_count": len(user_topics), "topics": sorted(user_topics)}
    except Exception as e:
        return {"healthy": False, "error": str(e)}


def check_postgres_health(dsn: str) -> dict[str, Any]:
    """Try a Postgres connection. Returns healthy=True/False."""
    try:
        import psycopg
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
        return {"healthy": True, "version": version}
    except Exception as e:
        return {"healthy": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Kafka inspection
# ---------------------------------------------------------------------------

def get_kafka_message_count(bootstrap_servers: str, topic: str) -> dict[str, Any]:
    """Return the total number of messages currently in a Kafka topic."""
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        meta = admin.list_topics(timeout=10)
        if topic not in meta.topics:
            return {"topic": topic, "message_count": 0, "exists": False}

        partitions = list(meta.topics[topic].partitions.keys())
        consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": "mpa-test-agent-inspector",
        })
        total = 0
        for p in partitions:
            lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            total += max(0, hi - lo)
        consumer.close()
        return {"topic": topic, "message_count": total, "exists": True}
    except Exception as e:
        return {"topic": topic, "message_count": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# File checks
# ---------------------------------------------------------------------------

def check_itch_file(file_path: str) -> dict[str, Any]:
    """Check whether an ITCH binary file exists and report its size."""
    p = Path(file_path)
    if p.exists():
        size_mb = p.stat().st_size / (1024 * 1024)
        return {"exists": True, "path": str(p.resolve()), "size_mb": round(size_mb, 1)}
    return {"exists": False, "path": str(p.resolve())}


# ---------------------------------------------------------------------------
# Process runners
# ---------------------------------------------------------------------------

def run_itch_runner(
    date: str,
    kafka: str,
    file_path: str | None = None,
    stocks: list[str] | None = None,
    max_msgs: int = 0,
    timeout_seconds: int = 600,
) -> dict[str, Any]:
    """
    Run the ITCH 5.0 feed parser and publisher.

    Args:
        date:            Business date in MMDDYYYY format. Used as trade_date for all
                         published messages AND for Kafka topic cleanup at startup.
                         Use a test isolation date (e.g. '01011970') so all data lands
                         in a dedicated Postgres partition.
        kafka:           Kafka bootstrap servers (e.g. 'localhost:9092').
        file_path:       Path to the actual ITCH binary file. If omitted, defaults to
                         ./data/{date}.NASDAQ_ITCH50.
        stocks:          Optional list of stock symbols to filter (e.g. ['AAPL', 'MSFT']).
        max_msgs:        Stop after this many messages (0 = process entire file).
                         Use a non-zero value for quick smoke tests; set 0 for NOII testing
                         since NOII messages only appear near the open/close.
        timeout_seconds: Kill the process after this many seconds (default 600 = 10 min).
    """
    cmd = [sys.executable, "-m", "itch.itch_runner", "--date", date, "--kafka", kafka]
    if file_path:
        cmd += ["--file", file_path]
    if stocks:
        cmd += ["--stocks"] + stocks
    if max_msgs:
        cmd += ["--max-msgs", str(max_msgs)]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cwd=str(Path(_SRC_DIR).parent),  # project root
            env=_env(),
        )
        return {
            "success": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": _trunc(result.stdout),
            "stderr": _trunc(result.stderr, 1000),
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": f"itch_runner timed out after {timeout_seconds}s",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_db_consumer(
    date: str,
    kafka: str,
    dsn: str,
    batch_size: int = 5000,
    flush_interval: float = 2.0,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """
    Run the Kafka-to-Postgres consumer.

    The consumer is a long-running process. This function starts it, lets it run
    for `timeout_seconds`, then sends SIGTERM so it performs a final flush before
    exiting. A graceful shutdown (returncode 0 or -15) is treated as success.

    Args:
        date:             Business date in MMDDYYYY format.  Must match the date used
                          with itch_runner so Postgres partitions align.
        kafka:            Kafka bootstrap servers.
        dsn:              Postgres DSN (e.g. 'postgresql://mpa:mpa@localhost:5432/mpa').
        batch_size:       Number of Kafka messages to buffer before flushing to Postgres.
        flush_interval:   Max seconds between Postgres flushes.
        timeout_seconds:  How long to let the consumer run before sending SIGTERM.
                          For large files, increase this value.
    """
    cmd = [
        sys.executable, "-m", "consumers.db_consumer",
        "--date", date,
        "--kafka", kafka,
        "--dsn", dsn,
        "--batch-size", str(batch_size),
        "--flush-interval", str(flush_interval),
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(_SRC_DIR).parent),
        env=_env(),
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_seconds)
        return {
            "success": proc.returncode in (0, -15),
            "returncode": proc.returncode,
            "stdout": _trunc(stdout),
            "stderr": _trunc(stderr, 500),
        }
    except subprocess.TimeoutExpired:
        # Graceful shutdown: SIGTERM → wait up to 15s → SIGKILL if needed
        proc.send_signal(_signal.SIGTERM)
        try:
            stdout, stderr = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
        return {
            "success": True,
            "returncode": proc.returncode,
            "stdout": _trunc(stdout),
            "stderr": _trunc(stderr, 500),
            "note": f"Consumer ran for {timeout_seconds}s then was gracefully stopped (expected).",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

def get_table_schema(dsn: str, table_name: str) -> dict[str, Any]:
    """
    Return column names, types, and nullability for a Postgres table.

    Queries information_schema.columns. Returns exists=False if the table has no
    columns (does not exist or was never created).

    Call this after cleanup_test_data to confirm the table exists and has the
    expected columns before running verify_queries. A missing table means the
    feature's schema migration hasn't been applied.
    """
    try:
        import psycopg
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                cols = [
                    {"name": r[0], "type": r[1], "nullable": r[2] == "YES"}
                    for r in cur.fetchall()
                ]
        return {"table": table_name, "exists": len(cols) > 0, "columns": cols}
    except Exception as e:
        return {"table": table_name, "exists": False, "error": str(e)}


def run_sql(dsn: str, query: str, params: dict | None = None) -> dict[str, Any]:
    """
    Execute a SQL query against Postgres and return column names + rows.

    Use %(name)s placeholders for parameters. Example:
        run_sql(dsn, "SELECT count(*) FROM noii WHERE trade_date = %(date)s",
                {"date": "1970-01-01"})
    """
    try:
        import psycopg
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or {})
                if cur.description:
                    cols = [d.name for d in cur.description]
                    rows = [list(r) for r in cur.fetchall()]
                    return {"columns": cols, "rows": rows, "row_count": len(rows)}
                return {"affected": cur.rowcount}
    except Exception as e:
        return {"error": str(e)}


def cleanup_test_data(dsn: str, test_date: str, tables: list[str]) -> dict[str, Any]:
    """
    Delete all rows for the given test date from the specified tables.

    Args:
        dsn:       Postgres DSN.
        test_date: Business date in MMDDYYYY format (e.g. '01011970').
        tables:    List of table names to clean (e.g. ['trades', 'noii']).

    Returns a dict with the number of rows deleted from each table.
    """
    deleted: dict[str, int] = {}
    try:
        import psycopg
        date_obj = datetime.strptime(test_date, "%m%d%Y").date()
        with psycopg.connect(dsn) as conn:
            for table in tables:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table} WHERE trade_date = %s", (date_obj,))
                    deleted[table] = cur.rowcount
            conn.commit()
        return {"deleted": deleted, "trade_date": str(date_obj)}
    except Exception as e:
        return {"error": str(e), "deleted": deleted}


# ---------------------------------------------------------------------------
# Tool schema definitions (passed to the Anthropic API)
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "name": "check_kafka_health",
        "description": (
            "Check whether Kafka is reachable. Returns healthy=true/false and a list of "
            "existing topics. Call this first in every test run."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "bootstrap_servers": {
                    "type": "string",
                    "description": "Kafka bootstrap servers, e.g. 'localhost:9092'",
                },
            },
            "required": ["bootstrap_servers"],
        },
    },
    {
        "name": "check_postgres_health",
        "description": (
            "Check whether Postgres is reachable. Returns healthy=true/false and the server version."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dsn": {
                    "type": "string",
                    "description": "Postgres DSN, e.g. 'postgresql://mpa:mpa@localhost:5432/mpa'",
                },
            },
            "required": ["dsn"],
        },
    },
    {
        "name": "check_itch_file",
        "description": "Check whether an ITCH binary data file exists and return its size.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Absolute or relative path to the ITCH file.",
                },
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "get_kafka_message_count",
        "description": (
            "Return the total number of messages currently in a Kafka topic by reading "
            "the high-water mark offsets. Use this after running itch_runner to confirm "
            "messages were produced."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "bootstrap_servers": {"type": "string"},
                "topic": {"type": "string", "description": "Kafka topic name, e.g. 'noii'"},
            },
            "required": ["bootstrap_servers", "topic"],
        },
    },
    {
        "name": "run_itch_runner",
        "description": (
            "Run the MPA ITCH 5.0 feed parser. Reads an ITCH binary file, decodes all "
            "market data messages, and publishes them to Kafka topics (trades, tob, vwap, noii). "
            "The process deletes and recreates all Kafka topics at startup for a clean slate. "
            "IMPORTANT for NOII testing: NOII messages only appear during opening cross "
            "(~9:25–9:30 ET) and closing cross (~3:50–4:00 ET). Do NOT use max_msgs=0 "
            "or a very small limit when testing NOII — the entire file (or a large portion) "
            "must be processed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": (
                        "Business date in MMDDYYYY format. "
                        "Use the TEST isolation date (e.g. '01011970') so all published "
                        "messages get trade_date='1970-01-01' in Postgres, keeping test "
                        "data separate from production data."
                    ),
                },
                "kafka": {
                    "type": "string",
                    "description": "Kafka bootstrap servers, e.g. 'localhost:9092'",
                },
                "file_path": {
                    "type": "string",
                    "description": (
                        "Path to the actual ITCH binary file (e.g. './data/04012024.NASDAQ_ITCH50'). "
                        "Required when using a test isolation date that differs from the file name."
                    ),
                },
                "stocks": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of stock symbols to filter, e.g. ['AAPL', 'MSFT']",
                },
                "max_msgs": {
                    "type": "integer",
                    "description": (
                        "Stop after this many ITCH messages (0 = process entire file). "
                        "Use 0 for NOII, or a large number (e.g. 50000000) for a full day."
                    ),
                },
                "timeout_seconds": {
                    "type": "integer",
                    "description": "Max seconds before killing the process (default 600).",
                },
            },
            "required": ["date", "kafka"],
        },
    },
    {
        "name": "run_db_consumer",
        "description": (
            "Run the Kafka-to-Postgres consumer. Reads messages from all four Kafka topics "
            "(trades, tob, vwap, noii) and writes them to Postgres partitioned by trade_date. "
            "The consumer runs until timeout_seconds then is gracefully stopped via SIGTERM. "
            "Use the same date as itch_runner so the Postgres partitions align."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Business date in MMDDYYYY format (must match itch_runner date).",
                },
                "kafka": {"type": "string", "description": "Kafka bootstrap servers."},
                "dsn": {"type": "string", "description": "Postgres DSN."},
                "batch_size": {
                    "type": "integer",
                    "description": "Messages to buffer before flushing to Postgres (default 5000).",
                },
                "flush_interval": {
                    "type": "number",
                    "description": "Max seconds between Postgres flushes (default 2.0).",
                },
                "timeout_seconds": {
                    "type": "integer",
                    "description": (
                        "How long to let the consumer run before SIGTERM. "
                        "For full-day ITCH files with all features, use 300+. "
                        "For limited --max-msgs runs, 60–120 is usually enough."
                    ),
                },
            },
            "required": ["date", "kafka", "dsn"],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Execute a SQL query against Postgres and return the results as column names + rows. "
            "Use %(name)s placeholders for parameters. "
            "Example: SELECT count(*) FROM noii WHERE trade_date = %(date)s"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dsn": {"type": "string"},
                "query": {"type": "string", "description": "SQL query with %(name)s placeholders."},
                "params": {
                    "type": "object",
                    "description": "Query parameters as a dict, e.g. {\"date\": \"1970-01-01\"}",
                },
            },
            "required": ["dsn", "query"],
        },
    },
    {
        "name": "cleanup_test_data",
        "description": (
            "Delete all rows for the test isolation date from the specified tables. "
            "Call this before running a test to ensure a clean slate."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dsn": {"type": "string"},
                "test_date": {
                    "type": "string",
                    "description": "Test isolation date in MMDDYYYY format (e.g. '01011970').",
                },
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Table names to clean, e.g. ['trades', 'noii'].",
                },
            },
            "required": ["dsn", "test_date", "tables"],
        },
    },
    {
        "name": "get_table_schema",
        "description": (
            "Return column names, types, and nullability for a Postgres table. "
            "Queries information_schema.columns. Returns exists=false if the table "
            "doesn't exist. Call this after cleanup_test_data for each table in the "
            "feature to confirm the schema is in place before running verify_queries. "
            "A missing table means the schema migration hasn't been applied."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dsn": {"type": "string"},
                "table_name": {
                    "type": "string",
                    "description": "Postgres table name, e.g. 'noii' or 'trades'.",
                },
            },
            "required": ["dsn", "table_name"],
        },
    },
]
