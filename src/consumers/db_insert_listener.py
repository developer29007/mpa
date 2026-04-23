import math

from analytics.VwapBucket import VwapBucket
from book.noii import Noii
from book.noii_listener import NoiiListener
from book.tob_listener import TobListener
from book.top_of_book import TopOfBook
from book.trade import Trade
from book.trade_listener import TradeListener
from db.inserter import DbInserter
from util.TimerListener import TimerListener
from util.TimerService import TimerService
from util.TimeUtil import ms_to_nanos
from util.message_id import next_id


def _trade_to_dict(trade: Trade) -> dict:
    return {
        "msg_id": next_id(),
        "timestamp_ns": trade.timestamp_ns,
        "sec_id": trade.sec_id,
        "shares": trade.shares,
        "price": trade.price,
        "side": trade.side,
        "trade_type": trade.type,
        "exch_id": trade.exch_id,
        "src": trade.src,
        "exch_match_id": trade.exch_match_id,
    }


def _tob_to_dict(tob: TopOfBook) -> dict:
    return {
        "msg_id": next_id(),
        "timestamp_ns": tob.timestamp,
        "stock": tob.name,
        "bid_price": tob.bid_price,
        "bid_size": tob.bid_size,
        "ask_price": tob.ask_price,
        "ask_size": tob.ask_size,
        "last_trade_price": tob.last_trade,
        "last_trade_timestamp_ns": tob.last_trade_timestamp,
        "last_trade_shares": tob.last_trade_shares,
        "last_trade_side": tob.last_trade_side,
        "last_trade_type": tob.last_trade_type,
        "last_trade_match_id": tob.last_trade_match_id,
    }


def _noii_to_dict(noii: Noii) -> dict:
    return {
        "msg_id": next_id(),
        "timestamp_ns": noii.timestamp_ns,
        "stock": noii.stock,
        "paired_shares": noii.paired_shares,
        "imbalance_shares": noii.imbalance_shares,
        "far_price": noii.far_price,
        "near_price": noii.near_price,
        "current_reference_price": noii.current_reference_price,
        "imbalance_direction": noii.imbalance_direction,
        "cross_type": noii.cross_type,
        "price_variation_indicator": noii.price_variation_indicator,
    }


def _bucket_to_vwap_dict(timestamp_ns: int, stock: str, bucket: VwapBucket) -> dict:
    vwap_price = bucket.vwap_price()
    return {
        "msg_id": next_id(),
        "timestamp_ns": timestamp_ns,
        "stock": stock,
        "interval_ms": bucket.interval_ms,
        "vwap_price": vwap_price if vwap_price is not None else math.nan,
        "volume_traded": bucket.volume_traded,
        "shares_traded": bucket.shares_traded,
        "trade_count": bucket.trade_count,
    }


class _VwapTimerHelper(TimerListener):
    """Computes VWAP for one interval, appending results to a shared buffer on each timer tick."""

    def __init__(self, interval_ms: int, vwap_buf: list):
        self._interval_ms = interval_ms
        self._interval_ns = ms_to_nanos(interval_ms)
        self._buckets: dict[str, VwapBucket] = {}
        self._vwap_buf = vwap_buf
        self._timer_registered = False

    def add_trade(self, trade: Trade) -> None:
        bucket = self._buckets.get(trade.sec_id)
        if bucket is None:
            bucket = VwapBucket(self._interval_ms)
            self._buckets[trade.sec_id] = bucket
        bucket.add_trade(trade)
        if not self._timer_registered:
            TimerService.instance().add_timer(self._interval_ns, trade.timestamp_ns, self)
            self._timer_registered = True

    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int) -> None:
        publish_time = scheduled_time + time_interval
        for stock, bucket in self._buckets.items():
            self._vwap_buf.append(_bucket_to_vwap_dict(publish_time, stock, bucket))
        TimerService.instance().add_timer(time_interval, publish_time, self)


class DbInsertListener(TradeListener, TobListener, NoiiListener):
    """Buffers market events and flushes them to the database via DbInserter.

    Two usage modes:
    - Direct (itch_runner --db): register with ItchFeedHandler; receives domain objects
      via on_trade / on_tob_change / on_noii. VWAP is computed internally via timers.
    - Kafka (db_consumer): call buffer_*_dict() with pre-deserialized dicts; original
      msg_ids from Kafka are preserved for idempotent inserts.
    """

    def __init__(self, inserter: DbInserter, interval_ms_list: list[int] | None = None,
                 batch_size: int = 1000):
        self._inserter = inserter
        self._batch_size = batch_size
        self._trade_buf: list[dict] = []
        self._vwap_buf: list[dict] = []
        self._tob_buf: list[dict] = []
        self._noii_buf: list[dict] = []
        self._vwap_timers = [_VwapTimerHelper(ms, self._vwap_buf) for ms in (interval_ms_list or [])]

    # --- Domain object interface (direct/itch_runner mode) ---

    def on_trade(self, trade: Trade) -> None:
        self._trade_buf.append(_trade_to_dict(trade))
        for timer in self._vwap_timers:
            timer.add_trade(trade)
        self._maybe_flush()

    def on_tob_change(self, tob: TopOfBook) -> None:
        self._tob_buf.append(_tob_to_dict(tob))
        self._maybe_flush()

    def on_noii(self, noii: Noii) -> None:
        self._noii_buf.append(_noii_to_dict(noii))
        self._maybe_flush()

    # --- Dict interface (Kafka mode — preserves original msg_ids) ---
    # These do NOT trigger _maybe_flush so that the Kafka polling loop controls when to
    # flush (it must also commit Kafka offsets after each flush).

    def buffer_trade_dict(self, d: dict) -> None:
        self._trade_buf.append(d)

    def buffer_vwap_dict(self, d: dict) -> None:
        self._vwap_buf.append(d)

    def buffer_tob_dict(self, d: dict) -> None:
        self._tob_buf.append(d)

    def buffer_noii_dict(self, d: dict) -> None:
        self._noii_buf.append(d)

    @property
    def pending_count(self) -> int:
        return len(self._trade_buf) + len(self._vwap_buf) + len(self._tob_buf) + len(self._noii_buf)

    # --- Flush ---

    def _maybe_flush(self) -> None:
        total = len(self._trade_buf) + len(self._vwap_buf) + len(self._tob_buf) + len(self._noii_buf)
        if total >= self._batch_size:
            self.flush()

    def flush(self) -> tuple[int, int, int, int]:
        """Flush all buffered records to the DB. Returns (trades, vwaps, tobs, noii) counts."""
        counts = (len(self._trade_buf), len(self._vwap_buf), len(self._tob_buf), len(self._noii_buf))
        if not any(counts):
            return counts
        self._inserter.flush(self._trade_buf, self._vwap_buf, self._tob_buf, self._noii_buf)
        self._trade_buf.clear()
        self._vwap_buf.clear()
        self._tob_buf.clear()
        self._noii_buf.clear()
        return counts
