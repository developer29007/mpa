import json
import os
from contextlib import asynccontextmanager

import psycopg
from mcp.server.fastmcp import FastMCP, Context


@asynccontextmanager
async def app_lifespan(server: FastMCP):
    dsn = os.environ["MPA_DSN"]
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        yield {"db": conn}
    finally:
        conn.close()


mcp = FastMCP("mpa-analytics", lifespan=app_lifespan)


def _get_db(ctx: Context) -> psycopg.Connection:
    return ctx.request_context.lifespan_context["db"]


def _rows_to_json(cursor) -> str:
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return json.dumps(rows, default=str, indent=2)


@mcp.tool()
def get_trades(
    stock: str,
    start_time: str,
    end_time: str,
    trade_date: str = "2019-10-30",
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Get trades for a stock in a time window.

    Args:
        stock: Stock symbol (e.g. "AAPL", "TSLA")
        start_time: Start time as HH:MM:SS or HH:MM:SS.mmm (e.g. "10:00:00")
        end_time: End time as HH:MM:SS or HH:MM:SS.mmm (e.g. "10:05:00")
        trade_date: Trading date as YYYY-MM-DD (default: "2019-10-30")
        limit: Max rows to return (default: 100)
    """
    db = _get_db(ctx)
    with db.cursor() as cur:
        cur.execute("""
            SELECT ns_to_time_us(timestamp_ns) AS time,
                   sec_id, shares, price, side, trade_type, exch_id, src
            FROM trades
            WHERE trade_date = %(trade_date)s
              AND sec_id = %(stock)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            LIMIT %(limit)s
        """, {"trade_date": trade_date, "stock": stock,
              "start": start_time, "end": end_time, "limit": limit})
        result = _rows_to_json(cur)
    db.rollback()
    return result


@mcp.tool()
def get_tob(
    stock: str,
    start_time: str,
    end_time: str,
    trade_date: str = "2019-10-30",
    limit: int = 100,
    ctx: Context = None,
) -> str:
    """Get top-of-book snapshots for a stock in a time window.
    Shows bid/ask prices and sizes, plus last trade info.

    Args:
        stock: Stock symbol (e.g. "AAPL", "TSLA")
        start_time: Start time as HH:MM:SS or HH:MM:SS.mmm (e.g. "10:00:00")
        end_time: End time as HH:MM:SS or HH:MM:SS.mmm (e.g. "10:05:00")
        trade_date: Trading date as YYYY-MM-DD (default: "2019-10-30")
        limit: Max rows to return (default: 100)
    """
    db = _get_db(ctx)
    with db.cursor() as cur:
        cur.execute("""
            SELECT ns_to_time_us(timestamp_ns) AS time,
                   bid_price, bid_size, ask_price, ask_size,
                   (ask_price - bid_price) AS spread,
                   last_trade_price, last_trade_shares, last_trade_side
            FROM tob
            WHERE trade_date = %(trade_date)s
              AND stock = %(stock)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            LIMIT %(limit)s
        """, {"trade_date": trade_date, "stock": stock,
              "start": start_time, "end": end_time, "limit": limit})
        result = _rows_to_json(cur)
    db.rollback()
    return result


@mcp.tool()
def get_vwap(
    stock: str,
    start_time: str,
    end_time: str,
    interval_ms: int = 1000,
    trade_date: str = "2019-10-30",
    limit: int = 500,
    ctx: Context = None,
) -> str:
    """Get VWAP snapshots for a stock in a time window.

    Args:
        stock: Stock symbol (e.g. "AAPL", "TSLA")
        start_time: Start time as HH:MM:SS (e.g. "10:00:00")
        end_time: End time as HH:MM:SS (e.g. "11:00:00")
        interval_ms: VWAP bucket interval in milliseconds (default: 1000). Available: 250, 1000, 2000, 5000, 10000, 20000
        trade_date: Trading date as YYYY-MM-DD (default: "2019-10-30")
        limit: Max rows to return (default: 500)
    """
    db = _get_db(ctx)
    with db.cursor() as cur:
        cur.execute("""
            SELECT ns_to_time(timestamp_ns) AS time,
                   interval_ms, vwap_price, volume_traded, shares_traded, trade_count
            FROM vwap
            WHERE trade_date = %(trade_date)s
              AND stock = %(stock)s
              AND interval_ms = %(interval_ms)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            LIMIT %(limit)s
        """, {"trade_date": trade_date, "stock": stock,
              "interval_ms": interval_ms,
              "start": start_time, "end": end_time, "limit": limit})
        result = _rows_to_json(cur)
    db.rollback()
    return result


@mcp.tool()
def query_market_data(
    sql: str,
    ctx: Context = None,
) -> str:
    """Run a read-only SQL query against the market data database.
    Use this for ad-hoc analysis, aggregations, cross-stock comparisons, or joins.

    Available tables (all partitioned by trade_date — always include it in WHERE):
      - trades: trade_date, msg_id, timestamp_ns, sec_id, shares, price, side, trade_type, exch_id, src, exch_match_id
      - vwap: trade_date, msg_id, timestamp_ns, stock, interval_ms, vwap_price, volume_traded, shares_traded, trade_count
      - tob: trade_date, msg_id, timestamp_ns, stock, bid_price, bid_size, ask_price, ask_size, last_trade_price, last_trade_timestamp_ns, last_trade_shares, last_trade_side, last_trade_type, last_trade_match_id

    Helper functions:
      - time_to_ns('HH:MM:SS') → BIGINT nanoseconds since midnight
      - ns_to_time(ns) → 'HH:MM:SS.mmm' (millisecond precision)
      - ns_to_time_us(ns) → 'HH:MM:SS.uuuuuu' (microsecond precision)

    Args:
        sql: A SELECT query. Only read-only queries are allowed.
    """
    normalized = sql.strip().lstrip("(").upper()
    if not normalized.startswith("SELECT") and not normalized.startswith("WITH"):
        return "Error: Only SELECT queries are allowed."

    db = _get_db(ctx)
    try:
        with db.cursor() as cur:
            cur.execute(sql)
            result = _rows_to_json(cur)
        db.rollback()
        return result
    except Exception as e:
        db.rollback()
        return f"Error: {e}"
