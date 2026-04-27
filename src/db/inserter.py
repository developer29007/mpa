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

NOII_INSERT = """
    INSERT INTO noii (
        trade_date, msg_id, timestamp_ns, stock, paired_shares, imbalance_shares,
        imbalance_direction, far_price, near_price, current_reference_price,
        cross_type, price_variation_indicator
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(stock)s, %(paired_shares)s, %(imbalance_shares)s,
        %(imbalance_direction)s, %(far_price)s, %(near_price)s, %(current_reference_price)s,
        %(cross_type)s, %(price_variation_indicator)s
    ) ON CONFLICT (msg_id, trade_date) DO NOTHING
"""

MARKET_EVENT_INSERT = """
    INSERT INTO market_events (
        trade_date, msg_id, timestamp_ns, event_type, stock, reason
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(event_type)s, %(stock)s, %(reason)s
    ) ON CONFLICT (msg_id, trade_date) DO NOTHING
"""

TRADE_BUCKET_INSERT = """
    INSERT INTO trade_buckets (
        trade_date, msg_id, timestamp_ns, stock, interval_ms,
        open, high, low, close, notional, vwap,
        total_shares, buy_shares, sell_shares, auction_shares, hidden_shares, trade_count,
        buy_volume, sell_volume, auction_volume, hidden_volume
    ) VALUES (
        %(trade_date)s, %(msg_id)s, %(timestamp_ns)s, %(stock)s, %(interval_ms)s,
        %(open)s, %(high)s, %(low)s, %(close)s, %(notional)s, %(vwap)s,
        %(total_shares)s, %(buy_shares)s, %(sell_shares)s, %(auction_shares)s, %(hidden_shares)s, %(trade_count)s,
        %(buy_volume)s, %(sell_volume)s, %(auction_volume)s, %(hidden_volume)s
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

    def insert_noii(self, noii_records: list[dict]) -> None:
        if not noii_records:
            return
        for n in noii_records:
            n["trade_date"] = self._trade_date
            n["far_price"] = _nan_to_none(n["far_price"])
            n["near_price"] = _nan_to_none(n["near_price"])
        with self._conn.cursor() as cur:
            cur.executemany(NOII_INSERT, noii_records)

    def insert_market_events(self, events: list[dict]) -> None:
        if not events:
            return
        for e in events:
            e["trade_date"] = self._trade_date
        with self._conn.cursor() as cur:
            cur.executemany(MARKET_EVENT_INSERT, events)

    def insert_trade_buckets(self, trade_buckets: list[dict]) -> None:
        if not trade_buckets:
            return
        for tb in trade_buckets:
            tb["trade_date"] = self._trade_date
            tb["vwap"] = _nan_to_none(tb["vwap"])
        with self._conn.cursor() as cur:
            cur.executemany(TRADE_BUCKET_INSERT, trade_buckets)

    def flush(self, trades: list[dict], vwaps: list[dict], tobs: list[dict],
              noii_records: list[dict] | None = None,
              market_events: list[dict] | None = None,
              trade_buckets: list[dict] | None = None) -> None:
        """Insert all buffered records in a single transaction, then commit."""
        self.insert_trades(trades)
        self.insert_vwaps(vwaps)
        self.insert_tobs(tobs)
        self.insert_noii(noii_records or [])
        self.insert_market_events(market_events or [])
        self.insert_trade_buckets(trade_buckets or [])
        self._conn.commit()
