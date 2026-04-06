import psycopg
from fastapi import APIRouter, Depends

from src.api.db import get_conn

router = APIRouter()


def _rows_to_dicts(cursor: psycopg.Cursor) -> list[dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@router.get("")
def get_vwap(
    symbol: str,
    start_time: str,
    end_time: str,
    trade_date: str = "2019-10-30",
    interval_ms: int = 1000,
    conn: psycopg.Connection = Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ns_to_time(timestamp_ns) AS time,
                   interval_ms, vwap_price, volume_traded, shares_traded, trade_count
            FROM vwap
            WHERE trade_date = %(trade_date)s
              AND stock = %(stock)s
              AND interval_ms = %(interval_ms)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            """,
            {"trade_date": trade_date, "stock": symbol,
             "interval_ms": interval_ms,
             "start": start_time, "end": end_time},
        )
        return _rows_to_dicts(cur)
