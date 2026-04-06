from typing import Annotated

import psycopg
from fastapi import APIRouter, Depends, Query

from src.api.db import get_conn

router = APIRouter()


def _rows_to_dicts(cursor: psycopg.Cursor) -> list[dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@router.get("")
def get_trades(
    symbol: str,
    start_time: str,
    end_time: str,
    trade_date: str = "2019-10-30",
    conn: psycopg.Connection = Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp_ns / 1e9          AS sec,
                   price,
                   shares,
                   ns_to_time_us(timestamp_ns) AS time_str,
                   side
            FROM trades
            WHERE trade_date = %(trade_date)s
              AND sec_id = %(stock)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            """,
            {"trade_date": trade_date, "stock": symbol,
             "start": start_time, "end": end_time},
        )
        return _rows_to_dicts(cur)
