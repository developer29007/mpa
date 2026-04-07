import psycopg
from fastapi import APIRouter, Depends

from src.api.db import get_conn

router = APIRouter()


def _rows_to_dicts(cursor: psycopg.Cursor) -> list[dict]:
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


@router.get("")
def get_tob(
    symbol: str,
    start_time: str,
    end_time: str,
    trade_date: str = "2019-10-30",
    conn: psycopg.Connection = Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ns_to_time_us(timestamp_ns) AS time,
                   bid_price, bid_size, ask_price, ask_size,
                   (ask_price - bid_price) AS spread,
                   last_trade_price, last_trade_shares, last_trade_side
            FROM tob
            WHERE trade_date = %(trade_date)s
              AND stock = %(stock)s
              AND timestamp_ns BETWEEN time_to_ns(%(start)s) AND time_to_ns(%(end)s)
            ORDER BY timestamp_ns
            """,
            {"trade_date": trade_date, "stock": symbol,
             "start": start_time, "end": end_time},
        )
        return _rows_to_dicts(cur)
