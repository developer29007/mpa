import struct

from book.market_event import MarketEvent
from book.market_event_listener import MarketEventListener
from publishers.kafka_publisher import KafkaPublisher
from util.message_id import next_id

# Big-endian binary struct format for a serialized market event message. Format:
#   Q  = unsigned 64-bit int  → msg_id
#   Q  = unsigned 64-bit int  → timestamp_ns
#   8s = 8-byte char string   → stock (left-justified)
#   16s= 16-byte char string  → event_type (e.g. 'OPEN_CROSS', 'HALT')
#   d  = 64-bit float (double)→ price (auction cross price; NaN for halt/resume)
#   Q  = unsigned 64-bit int  → shares (auction volume; 0 for halt/resume)
#   4s = 4-byte char string   → reason (halt reason code, e.g. 'LUDP')
MARKET_EVENT_FORMAT = '>QQ8s16sdQ4s'
MARKET_EVENT_MSG_TYPE = 'M'


def _serialize_market_event(event: MarketEvent) -> bytes:
    stock = event.stock.encode('ascii').ljust(8)[:8]
    event_type = event.event_type.encode('ascii').ljust(16)[:16]
    reason = event.reason.encode('ascii').ljust(4)[:4]
    return struct.pack(
        MARKET_EVENT_FORMAT,
        next_id(),
        event.timestamp_ns,
        stock,
        event_type,
        event.price,
        event.shares,
        reason,
    )


class MarketEventPublisher(MarketEventListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)

    def on_market_event(self, event: MarketEvent):
        payload = _serialize_market_event(event)
        self._publish(MARKET_EVENT_MSG_TYPE, payload)
