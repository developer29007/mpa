import math
import struct

from book.tob_listener import TobListener
from book.top_of_book import TopOfBook
from publishers.kafka_publisher import KafkaPublisher
from util.message_id import next_id

# Big-endian (>) binary struct format for a serialized top-of-book message. Format characters:
#   Q  = unsigned 64-bit int  → msg_id (globally unique message identifier)
#   Q  = unsigned 64-bit int  → timestamp_ns (when TOB changed)
#   8s = 8-byte char string   → stock (security identifier, left-justified)
#   d  = 64-bit float (double)→ bid_price (NaN if no bids)
#   I  = unsigned 32-bit int  → bid_size (0 if no bids)
#   d  = 64-bit float (double)→ ask_price (NaN if no asks)
#   I  = unsigned 32-bit int  → ask_size (0 if no asks)
#   d  = 64-bit float (double)→ last_trade_price (NaN if no trades yet)
#   Q  = unsigned 64-bit int  → last_trade_timestamp_ns (0 if no trades yet)
#   I  = unsigned 32-bit int  → last_trade_shares (0 if no trades yet)
#   c  = 1-byte char          → last_trade_side ('B'/'S', space if no trades yet)
#   c  = 1-byte char          → last_trade_type (space if no trades yet)
#   Q  = unsigned 64-bit int  → last_trade_match_id (0 if no trades yet)
TOB_FORMAT = '>QQ8sdIdIdQIccQ'
TOB_MSG_TYPE = 'B'


def _serialize_tob(tob: TopOfBook) -> bytes:
    stock_bytes = tob.name.encode('ascii').ljust(8)
    bid_price = tob.bid_price / 10000 if tob.bid_price is not None else math.nan
    ask_price = tob.ask_price / 10000 if tob.ask_price is not None else math.nan
    last_trade = tob.last_trade if tob.last_trade is not None else math.nan
    side = tob.last_trade_side.encode('ascii') if tob.last_trade_side else b' '
    trade_type = tob.last_trade_type.encode('ascii') if tob.last_trade_type else b' '
    return struct.pack(
        TOB_FORMAT,
        next_id(),
        tob.timestamp,
        stock_bytes,
        bid_price,
        tob.bid_size,
        ask_price,
        tob.ask_size,
        last_trade,
        tob.last_trade_timestamp,
        tob.last_trade_shares,
        side,
        trade_type,
        tob.last_trade_match_id,
    )


class TobPublisher(TobListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, topic: str):
        KafkaPublisher.__init__(self, bootstrap_servers, topic)

    def on_tob_change(self, tob: TopOfBook):
        payload = _serialize_tob(tob)
        self._publish(TOB_MSG_TYPE, payload)
