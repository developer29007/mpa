import datetime
import struct

from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher

TRADE_FORMAT = '>Q8sIdcc4s8sQI'
TRADE_MSG_TYPE = 'T'
EPOCH = datetime.date(1970, 1, 1)


def _serialize_trade(trade: Trade) -> bytes:
    sec_id = trade.sec_id.encode('ascii').ljust(8)
    side = trade.side.encode('ascii') if trade.side else b' '
    trade_type = trade.type.encode('ascii') if trade.type else b' '
    exch_id = trade.exch_id.encode('ascii').ljust(4)
    src = trade.src.encode('ascii').ljust(8)
    exch_match_id = int(trade.exch_match_id) if trade.exch_match_id else 0
    trade_date_days = (trade.trade_date - EPOCH).days if trade.trade_date else 0
    return struct.pack(
        TRADE_FORMAT,
        trade.timestamp_ns,
        sec_id,
        trade.shares,
        trade.price,
        side,
        trade_type,
        exch_id,
        src,
        exch_match_id,
        trade_date_days,
    )


class TradePublisher(TradeListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, trade_date: datetime.date):
        topic = f'trade-analytics-{trade_date.isoformat()}'
        KafkaPublisher.__init__(self, bootstrap_servers, topic)

    def on_trade(self, trade: Trade):
        payload = _serialize_trade(trade)
        self._publish(TRADE_MSG_TYPE, payload)
