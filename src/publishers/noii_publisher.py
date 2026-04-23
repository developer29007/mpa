import math
import struct

from book.noii import Noii
from book.noii_listener import NoiiListener
from publishers.kafka_publisher import KafkaPublisher
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized NOII message. Format characters:
#   Q  = unsigned 64-bit int  → msg_id
#   Q  = unsigned 64-bit int  → timestamp_ns
#   8s = 8-byte char string   → stock
#   Q  = unsigned 64-bit int  → paired_shares
#   Q  = unsigned 64-bit int  → imbalance_shares
#   d  = 64-bit float (double)→ far_price (NaN if not applicable)
#   d  = 64-bit float (double)→ near_price (NaN if not applicable)
#   d  = 64-bit float (double)→ current_reference_price
#   c  = 1-byte char          → imbalance_direction
#   c  = 1-byte char          → cross_type
#   c  = 1-byte char          → price_variation_indicator
NOII_FORMAT = '>QQ8sQQdddccc'
NOII_MSG_TYPE = 'I'


def _serialize_noii(noii: Noii) -> bytes:
    stock_bytes = noii.stock.encode('ascii').ljust(8)
    far_price = noii.far_price if noii.far_price is not None else math.nan
    near_price = noii.near_price if noii.near_price is not None else math.nan
    return struct.pack(
        NOII_FORMAT,
        next_id(),
        noii.timestamp_ns,
        stock_bytes,
        noii.paired_shares,
        noii.imbalance_shares,
        far_price,
        near_price,
        noii.current_reference_price,
        noii.imbalance_direction.encode('ascii'),
        noii.cross_type.encode('ascii'),
        noii.price_variation_indicator.encode('ascii'),
    )


class NoiiPublisher(NoiiListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)

    def on_noii(self, noii: Noii):
        payload = _serialize_noii(noii)
        self._publish(NOII_MSG_TYPE, payload)
