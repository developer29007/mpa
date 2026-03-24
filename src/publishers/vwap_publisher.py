import math
import struct

from analytics.VwapBucket import VwapBucket
from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher
from util.TimerListener import TimerListener
from util.TimerService import TimerService
from util.TimeUtil import ms_to_nanos
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized VWAP message. Format characters:
#   Q  = unsigned 64-bit int  → msg_id (globally unique message identifier)
#   Q  = unsigned 64-bit int  → timestamp_ns (aligned tick boundary, ns since midnight)
#   8s = 8-byte char string   → stock (security identifier, left-justified)
#   I  = unsigned 32-bit int  → interval_ms (bucket window in milliseconds)
#   d  = 64-bit float (double)→ vwap_price
#   d  = 64-bit float (double)→ volume_traded
#   I  = unsigned 32-bit int  → shares_traded
#   I  = unsigned 32-bit int  → trade_count
VWAP_FORMAT = '>QQ8sIddII'
VWAP_MSG_TYPE = 'V'


def _serialize_vwap(timestamp_ns: int, stock: str, bucket) -> bytes:
    stock_bytes = stock.encode('ascii').ljust(8)
    vwap_price = bucket.vwap_price() if bucket.vwap_price() is not None else math.nan
    return struct.pack(
        VWAP_FORMAT,
        next_id(),
        timestamp_ns,
        stock_bytes,
        bucket.interval_ms,
        vwap_price,
        bucket.volume_traded,
        bucket.shares_traded,
        bucket.trade_count,
    )


class VwapPublisher(TradeListener, TimerListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str, interval_ms: int):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)
        self._interval_ms = interval_ms
        self._interval_ns = ms_to_nanos(interval_ms)
        self._buckets: dict[str, VwapBucket] = {}
        self._timer_registered = False

    def on_trade(self, trade: Trade):
        bucket = self._buckets.get(trade.sec_id)
        if bucket is None:
            bucket = VwapBucket(self._interval_ms)
            self._buckets[trade.sec_id] = bucket
        bucket.add_trade(trade)

        if not self._timer_registered:
            TimerService.instance().add_timer(self._interval_ns, trade.timestamp_ns, self)
            self._timer_registered = True

    def on_timer_expired(self, time_interval: int, scheduled_time: int, time_now: int):
        publish_time = scheduled_time + time_interval
        for stock, bucket in self._buckets.items():
            payload = _serialize_vwap(publish_time, stock, bucket)
            self._publish(VWAP_MSG_TYPE, payload)
        TimerService.instance().add_timer(time_interval, publish_time, self)
