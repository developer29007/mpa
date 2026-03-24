import struct

from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized trade message. Format characters:
#   Q  = unsigned 64-bit int  → msg_id (globally unique message identifier)
#   Q  = unsigned 64-bit int  → timestamp_ns
#   8s = 8-byte char string   → sec_id (security identifier, left-justified)
#   I  = unsigned 32-bit int  → shares
#   d  = 64-bit float (double)→ price
#   c  = 1-byte char          → side (e.g. 'B'/'S')
#   c  = 1-byte char          → trade_type
#   4s = 4-byte char string   → exch_id (exchange identifier)
#   8s = 8-byte char string   → src (data source)
#   Q  = unsigned 64-bit int  → exch_match_id
TRADE_FORMAT = '>QQ8sIdcc4s8sQ'
TRADE_MSG_TYPE = 'T'


def _serialize_trade(trade: Trade) -> bytes:
    sec_id = trade.sec_id.encode('ascii').ljust(8)
    side = trade.side.encode('ascii') if trade.side else b' '
    trade_type = trade.type.encode('ascii') if trade.type else b' '
    exch_id = trade.exch_id.encode('ascii').ljust(4)
    src = trade.src.encode('ascii').ljust(8)
    exch_match_id = int(trade.exch_match_id) if trade.exch_match_id else 0
    return struct.pack(
        TRADE_FORMAT,
        next_id(),
        trade.timestamp_ns,
        sec_id,
        trade.shares,
        trade.price,
        side,
        trade_type,
        exch_id,
        src,
        exch_match_id,
    )


class TradePublisher(TradeListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)

    def on_trade(self, trade: Trade):
        payload = _serialize_trade(trade)
        self._publish(TRADE_MSG_TYPE, payload)
