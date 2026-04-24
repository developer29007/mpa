import math
import struct

from analytics.CandleBucket import CandleBucket
from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher
from util.TimerListener import TimerListener
from util.TimerService import TimerService
from util.TimeUtil import ms_to_nanos
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized candle message. Format characters:
#   Q  = unsigned 64-bit int  → msg_id
#   Q  = unsigned 64-bit int  → timestamp_ns (bucket START time, ns since midnight)
#   8s = 8-byte char string   → stock
#   I  = unsigned 32-bit int  → interval_ms
#   d  = 64-bit float         → open
#   d  = 64-bit float         → high
#   d  = 64-bit float         → low
#   d  = 64-bit float         → close
#   d  = 64-bit float         → dollar_volume (sum of price*shares; enables cross-bucket aggregation)
#   d  = 64-bit float         → vwap (dollar_volume / total_vol, or NaN if no trades)
#   I  = unsigned 32-bit int  → total_vol
#   I  = unsigned 32-bit int  → bid_vol   (sell-aggressor shares)
#   I  = unsigned 32-bit int  → offer_vol (buy-aggressor shares)
#   I  = unsigned 32-bit int  → auction_vol
#   I  = unsigned 32-bit int  → trade_count
CANDLE_FORMAT = '>QQ8sIddddddIIIII'
CANDLE_MSG_TYPE = 'C'


def _serialize_candle(stock: str, bucket: CandleBucket) -> bytes:
    stock_bytes = stock.encode('ascii').ljust(8)
    _vwap = bucket.vwap()
    vwap = _vwap if _vwap is not None else math.nan
    return struct.pack(
        CANDLE_FORMAT,
        next_id(),
        bucket.bucket_start_ns,
        stock_bytes,
        bucket.interval_ms,
        bucket.open,
        bucket.high,
        bucket.low,
        bucket.close,
        bucket.dollar_volume,
        vwap,
        bucket.total_vol,
        bucket.bid_vol,
        bucket.offer_vol,
        bucket.auction_vol,
        bucket.trade_count,
    )


class CandlePublisher(TradeListener, TimerListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str, interval_ms: int):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)
        self._interval_ms = interval_ms
        self._interval_ns = ms_to_nanos(interval_ms)
        self._buckets: dict[str, CandleBucket] = {}
        self._timer_registered = False

    def _calc_bucket_start_ns(self, timestamp_ns: int) -> int:
        return (timestamp_ns // self._interval_ns) * self._interval_ns

    def on_trade(self, trade: Trade):
        stock = trade.sec_id
        bucket_start_ns = self._calc_bucket_start_ns(trade.timestamp_ns)

        prev_bucket = self._buckets.get(stock)
        if prev_bucket is not None and prev_bucket.bucket_start_ns != bucket_start_ns:
            if not prev_bucket.is_empty:
                self._publish(CANDLE_MSG_TYPE, _serialize_candle(stock, prev_bucket))
            self._buckets[stock] = CandleBucket(self._interval_ms, bucket_start_ns)
        elif stock not in self._buckets:
            self._buckets[stock] = CandleBucket(self._interval_ms, bucket_start_ns)

        self._buckets[stock].add_trade(trade)

        if not self._timer_registered:
            first_boundary_ns = bucket_start_ns + self._interval_ns
            delay_ns = first_boundary_ns - trade.timestamp_ns
            TimerService.instance().add_timer(delay_ns, trade.timestamp_ns, self)
            self._timer_registered = True

    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int):
        boundary_ns = scheduled_time + time_interval

        for stock, bucket in self._buckets.items():
            if bucket.bucket_start_ns < boundary_ns and not bucket.is_empty:
                self._publish(CANDLE_MSG_TYPE, _serialize_candle(stock, bucket))
                self._buckets[stock] = CandleBucket(self._interval_ms, boundary_ns)

        TimerService.instance().add_timer(self._interval_ns, boundary_ns, self)

    def flush(self):
        for stock, bucket in self._buckets.items():
            if not bucket.is_empty:
                self._publish(CANDLE_MSG_TYPE, _serialize_candle(stock, bucket))
        super().flush()
