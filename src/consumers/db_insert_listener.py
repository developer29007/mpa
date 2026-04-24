import math

from analytics.CandleBucket import CandleBucket
from analytics.VwapBucket import VwapBucket
from book.market_event import MarketEvent
from book.market_event_listener import MarketEventListener
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


def _market_event_to_dict(event: MarketEvent) -> dict:
    return {
        "msg_id": next_id(),
        "timestamp_ns": event.timestamp_ns,
        "stock": event.stock,
        "event_type": event.event_type,
        "reason": event.reason,
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


def _bucket_to_candle_dict(bucket_start_ns: int, stock: str, bucket: CandleBucket) -> dict:
    vwap = bucket.vwap()
    return {
        "msg_id": next_id(),
        "timestamp_ns": bucket_start_ns,
        "stock": stock,
        "interval_ms": bucket.interval_ms,
        "open": bucket.open,
        "high": bucket.high,
        "low": bucket.low,
        "close": bucket.close,
        "dollar_volume": bucket.dollar_volume,
        "vwap": vwap if vwap is not None else math.nan,
        "total_vol": bucket.total_vol,
        "bid_vol": bucket.bid_vol,
        "offer_vol": bucket.offer_vol,
        "auction_vol": bucket.auction_vol,
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


class _CandleTimerHelper(TimerListener):
    """Computes epoch-aligned OHLCV candles for one interval, appending to a shared buffer.

    Uses the same boundary-crossing + timer dual-path as CandlePublisher: on_trade publishes
    completed buckets as new trades arrive, while the timer catches buckets for stocks that
    have no further trades after a boundary closes.
    """

    def __init__(self, interval_ms: int, candle_buf: list):
        self._interval_ms = interval_ms
        self._interval_ns = ms_to_nanos(interval_ms)
        self._buckets: dict[str, CandleBucket] = {}
        self._bucket_start: dict[str, int] = {}
        self._candle_buf = candle_buf
        self._timer_registered = False

    def _bucket_start_ns(self, timestamp_ns: int) -> int:
        return (timestamp_ns // self._interval_ns) * self._interval_ns

    def add_trade(self, trade: Trade) -> None:
        stock = trade.sec_id
        bucket_ns = self._bucket_start_ns(trade.timestamp_ns)

        prev_ns = self._bucket_start.get(stock)
        if prev_ns is not None and prev_ns != bucket_ns:
            bucket = self._buckets[stock]
            if not bucket.is_empty:
                self._candle_buf.append(_bucket_to_candle_dict(prev_ns, stock, bucket))
            self._buckets[stock] = CandleBucket(self._interval_ms)

        if stock not in self._buckets:
            self._buckets[stock] = CandleBucket(self._interval_ms)

        self._buckets[stock].add_trade(trade)
        self._bucket_start[stock] = bucket_ns

        if not self._timer_registered:
            first_boundary_ns = bucket_ns + self._interval_ns
            delay_ns = first_boundary_ns - trade.timestamp_ns
            TimerService.instance().add_timer(delay_ns, trade.timestamp_ns, self)
            self._timer_registered = True

    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int) -> None:
        boundary_ns = scheduled_time + time_interval
        for stock, bucket in self._buckets.items():
            bucket_start = self._bucket_start.get(stock, boundary_ns)
            if bucket_start < boundary_ns and not bucket.is_empty:
                self._candle_buf.append(_bucket_to_candle_dict(bucket_start, stock, bucket))
                self._buckets[stock] = CandleBucket(self._interval_ms)
                self._bucket_start[stock] = boundary_ns
        TimerService.instance().add_timer(self._interval_ns, boundary_ns, self)

    def flush_remaining(self) -> None:
        """Drain any still-open buckets into the buffer. Call only at end of run."""
        for stock, bucket in self._buckets.items():
            if not bucket.is_empty:
                bucket_ns = self._bucket_start.get(stock, 0)
                self._candle_buf.append(_bucket_to_candle_dict(bucket_ns, stock, bucket))
                self._buckets[stock] = CandleBucket(self._interval_ms)


class DbInsertListener(TradeListener, TobListener, NoiiListener, MarketEventListener):
    """Buffers market events and flushes them to the database via DbInserter.

    Two usage modes:
    - Direct (itch_runner --db): register with ItchFeedHandler; receives domain objects
      via on_trade / on_tob_change / on_noii. VWAP and candles are computed internally
      via timers.
    - Kafka (db_consumer): call buffer_*_dict() with pre-deserialized dicts; original
      msg_ids from Kafka are preserved for idempotent inserts.
    """

    def __init__(self, inserter: DbInserter,
                 vwap_interval_ms_list: list[int] | None = None,
                 candle_interval_ms_list: list[int] | None = None,
                 batch_size: int = 1000):
        self._inserter = inserter
        self._batch_size = batch_size
        self._trade_buf: list[dict] = []
        self._vwap_buf: list[dict] = []
        self._tob_buf: list[dict] = []
        self._noii_buf: list[dict] = []
        self._market_event_buf: list[dict] = []
        self._candle_buf: list[dict] = []
        self._vwap_timers = [_VwapTimerHelper(ms, self._vwap_buf) for ms in (vwap_interval_ms_list or [])]
        self._candle_timers = [_CandleTimerHelper(ms, self._candle_buf)
                               for ms in (candle_interval_ms_list or [])]

    # --- Domain object interface (direct/itch_runner mode) ---

    def on_trade(self, trade: Trade) -> None:
        self._trade_buf.append(_trade_to_dict(trade))
        for timer in self._vwap_timers:
            timer.add_trade(trade)
        for timer in self._candle_timers:
            timer.add_trade(trade)
        self._maybe_flush()

    def on_tob_change(self, tob: TopOfBook) -> None:
        self._tob_buf.append(_tob_to_dict(tob))
        self._maybe_flush()

    def on_noii(self, noii: Noii) -> None:
        self._noii_buf.append(_noii_to_dict(noii))
        self._maybe_flush()

    def on_market_event(self, event: MarketEvent) -> None:
        self._market_event_buf.append(_market_event_to_dict(event))
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

    def buffer_market_event_dict(self, d: dict) -> None:
        self._market_event_buf.append(d)

    def buffer_candle_dict(self, d: dict) -> None:
        self._candle_buf.append(d)

    @property
    def pending_count(self) -> int:
        return (len(self._trade_buf) + len(self._vwap_buf) + len(self._tob_buf)
                + len(self._noii_buf) + len(self._market_event_buf) + len(self._candle_buf))

    # --- Flush ---

    def _maybe_flush(self) -> None:
        if self.pending_count >= self._batch_size:
            self.flush()

    def flush(self, final: bool = False) -> tuple[int, int, int, int, int, int]:
        """Flush all buffered records to the DB.

        Returns (trades, vwaps, tobs, noii, market_events, candles) counts.
        Pass final=True at end of run to also drain still-open candle buckets.
        """
        if final:
            for timer in self._candle_timers:
                timer.flush_remaining()
        counts = (len(self._trade_buf), len(self._vwap_buf), len(self._tob_buf),
                  len(self._noii_buf), len(self._market_event_buf), len(self._candle_buf))
        if not any(counts):
            return counts
        self._inserter.flush(self._trade_buf, self._vwap_buf, self._tob_buf,
                             self._noii_buf, self._market_event_buf, self._candle_buf)
        self._trade_buf.clear()
        self._vwap_buf.clear()
        self._tob_buf.clear()
        self._noii_buf.clear()
        self._market_event_buf.clear()
        self._candle_buf.clear()
        return counts
