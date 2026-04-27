import math
import struct

from analytics.TradeBucket import TradeBucket
from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher
from util.TimerListener import TimerListener
from util.TimerService import TimerService
from util.TimeUtil import ms_to_nanos
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized trade-bucket message.
#   Q  = unsigned 64-bit int  → msg_id
#   Q  = unsigned 64-bit int  → timestamp_ns (bucket START time, ns since midnight)
#   8s = 8-byte char string   → stock
#   I  = unsigned 32-bit int  → interval_ms
#   d  = 64-bit float         → open
#   d  = 64-bit float         → high
#   d  = 64-bit float         → low
#   d  = 64-bit float         → close
#   d  = 64-bit float         → notional  (Σ price×shares, all trades)
#   d  = 64-bit float         → vwap      (notional / total_shares, or NaN)
#   I  = unsigned 32-bit int  → total_shares
#   I  = unsigned 32-bit int  → buy_shares    (buy-aggressor)
#   I  = unsigned 32-bit int  → sell_shares   (sell-aggressor)
#   I  = unsigned 32-bit int  → auction_shares (O/C/H cross)
#   I  = unsigned 32-bit int  → hidden_shares  (non-displayable / dark)
#   I  = unsigned 32-bit int  → trade_count
#   d  = 64-bit float         → buy_volume    (notional, buy-aggressor)
#   d  = 64-bit float         → sell_volume   (notional, sell-aggressor)
#   d  = 64-bit float         → auction_volume
#   d  = 64-bit float         → hidden_volume
TRADE_BUCKET_FORMAT = '>QQ8sIddddddIIIIIIdddd'
TRADE_BUCKET_MSG_TYPE = 'K'


def _serialize_trade_bucket(stock: str, bucket: TradeBucket) -> bytes:
    stock_bytes = stock.encode('ascii').ljust(8)
    _vwap = bucket.vwap()
    vwap = _vwap if _vwap is not None else math.nan
    return struct.pack(
        TRADE_BUCKET_FORMAT,
        next_id(),
        bucket.bucket_start_ns,
        stock_bytes,
        bucket.interval_ms,
        bucket.open,
        bucket.high,
        bucket.low,
        bucket.close,
        bucket.notional,
        vwap,
        bucket.total_shares,
        bucket.buy_shares,
        bucket.sell_shares,
        bucket.auction_shares,
        bucket.hidden_shares,
        bucket.trade_count,
        bucket.buy_volume,
        bucket.sell_volume,
        bucket.auction_volume,
        bucket.hidden_volume,
    )


class TradeBucketPublisher(TradeListener, TimerListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str, interval_ms: int):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)
        self._interval_ms = interval_ms
        self._interval_ns = ms_to_nanos(interval_ms)
        self._buckets: dict[str, TradeBucket] = {}
        self._timer_registered = False

    def _calc_bucket_start_ns(self, timestamp_ns: int) -> int:
        return (timestamp_ns // self._interval_ns) * self._interval_ns

    def on_trade(self, trade: Trade):
        stock = trade.sec_id
        bucket_start_ns = self._calc_bucket_start_ns(trade.timestamp_ns)

        prev_bucket = self._buckets.get(stock)
        if prev_bucket is not None and prev_bucket.bucket_start_ns != bucket_start_ns:
            if not prev_bucket.is_empty:
                self._publish(TRADE_BUCKET_MSG_TYPE, _serialize_trade_bucket(stock, prev_bucket))
            self._buckets[stock] = TradeBucket(self._interval_ms, bucket_start_ns)
        elif stock not in self._buckets:
            self._buckets[stock] = TradeBucket(self._interval_ms, bucket_start_ns)

        self._buckets[stock].add_trade(trade)

        if not self._timer_registered:
            first_boundary_ns = bucket_start_ns + self._interval_ns
            TimerService.instance().add_timer(first_boundary_ns - trade.timestamp_ns, trade.timestamp_ns, self)
            self._timer_registered = True

    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int):
        boundary_ns = scheduled_time + time_interval
        for stock, bucket in self._buckets.items():
            if bucket.bucket_start_ns < boundary_ns and not bucket.is_empty:
                self._publish(TRADE_BUCKET_MSG_TYPE, _serialize_trade_bucket(stock, bucket))
                self._buckets[stock] = TradeBucket(self._interval_ms, boundary_ns)
        next_boundary_ns = self._calc_bucket_start_ns(time_now) + self._interval_ns
        TimerService.instance().add_timer(next_boundary_ns - time_now, time_now, self)

    def flush(self):
        for stock, bucket in self._buckets.items():
            if not bucket.is_empty:
                self._publish(TRADE_BUCKET_MSG_TYPE, _serialize_trade_bucket(stock, bucket))
        super().flush()
