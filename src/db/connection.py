import datetime

import psycopg


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=False)


def ensure_partitions(conn: psycopg.Connection, trade_date: datetime.date) -> None:
    """Create daily partitions for trades, vwap, and tob if they don't exist."""
    iso = trade_date.isoformat()
    next_day = trade_date + datetime.timedelta(days=1)
    next_iso = next_day.isoformat()
    suffix = trade_date.strftime("%Y%m%d")

    for table in ("trades", "vwap", "tob", "market_events"):
        partition = f"{table}_{suffix}"
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition}
            PARTITION OF {table}
            FOR VALUES FROM ('{iso}') TO ('{next_iso}')
        """)
    conn.commit()
