import math
import datetime

import psycopg


def _nan_to_none(value: float):
    """Convert NaN floats to None for Postgres NULL storage."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


TRADE_INSERT = """
    INSERT INTO trades (
        trade_date, msg_id, timestamp_ns, sec_id, shares, price,
        side, trade_type, exch_id, src, exch_match_id
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(sec_id)s, %(shares)s, %(price)s,
        %(side)s, %(trade_type)s, %(exch_id)s, %(src)s, %(exch_match_id)s
    ) ON CONFLICT (msg_id, trade_date) DO NOTHING
"""

VWAP_INSERT = """
    INSERT INTO vwap (
        trade_date, msg_id, timestamp_ns, stock, interval_ms, vwap_price,
        volume_traded, shares_traded, trade_count
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(stock)s, %(interval_ms)s, %(vwap_price)s,
        %(volume_traded)s, %(shares_traded)s, %(trade_count)s
    ) ON CONFLICT (msg_id, trade_date) DO NOTHING
"""

TOB_INSERT = """
    INSERT INTO tob (
        trade_date, msg_id, timestamp_ns, stock, bid_price, bid_size,
        ask_price, ask_size, last_trade_price, last_trade_timestamp_ns,
        last_trade_shares, last_trade_side, last_trade_type, last_trade_match_id
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(stock)s, %(bid_price)s, %(bid_size)s,
        %(ask_price)s, %(ask_size)s, %(last_trade_price)s, %(last_trade_timestamp_ns)s,
        %(last_trade_shares)s, %(last_trade_side)s, %(last_trade_type)s, %(last_trade_match_id)s
    ) ON CONFLICT (msg_id, trade_date) DO NOTHING
"""


class DbInserter:

    def __init__(self, conn: psycopg.Connection, trade_date: datetime.date):
        self._conn = conn
        self._trade_date = trade_date

    def insert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return
        for t in trades:
            t["trade_date"] = self._trade_date
        with self._conn.cursor() as cur:
            cur.executemany(TRADE_INSERT, trades)

    def insert_vwaps(self, vwaps: list[dict]) -> None:
        if not vwaps:
            return
        for v in vwaps:
            v["trade_date"] = self._trade_date
            v["vwap_price"] = _nan_to_none(v["vwap_price"])
        with self._conn.cursor() as cur:
            cur.executemany(VWAP_INSERT, vwaps)

    def insert_tobs(self, tobs: list[dict]) -> None:
        if not tobs:
            return
        for t in tobs:
            t["trade_date"] = self._trade_date
            t["bid_price"] = _nan_to_none(t["bid_price"])
            t["ask_price"] = _nan_to_none(t["ask_price"])
            t["last_trade_price"] = _nan_to_none(t["last_trade_price"])
        with self._conn.cursor() as cur:
            cur.executemany(TOB_INSERT, tobs)

    def flush(self, trades: list[dict], vwaps: list[dict], tobs: list[dict]) -> None:
        """Insert all buffered records in a single transaction, then commit."""
        self.insert_trades(trades)
        self.insert_vwaps(vwaps)
        self.insert_tobs(tobs)
        self._conn.commit()
