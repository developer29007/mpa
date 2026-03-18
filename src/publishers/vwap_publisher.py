import datetime
import math
import struct

from analytics.VwapTracker import VwapTracker
from book.trade import Trade
from book.trade_listener import TradeListener
from publishers.kafka_publisher import KafkaPublisher

VWAP_FORMAT = '>Q8sIddII'
VWAP_MSG_TYPE = 'V'


def _serialize_vwap(timestamp_ns: int, stock: str, bucket) -> bytes:
    stock_bytes = stock.encode('ascii').ljust(8)
    vwap_price = bucket.vwap_price() if bucket.vwap_price() is not None else math.nan
    return struct.pack(
        VWAP_FORMAT,
        timestamp_ns,
        stock_bytes,
        bucket.interval_ms,
        vwap_price,
        bucket.volume_traded,
        bucket.shares_traded,
        bucket.trade_count,
    )


class VwapPublisher(TradeListener, KafkaPublisher):

    def __init__(self, bootstrap_servers: str, trade_date: datetime.date, bucket_intervals: list[int]):
        topic = f'vwap-analytics-{trade_date.isoformat()}'
        KafkaPublisher.__init__(self, bootstrap_servers, topic)
        self.bucket_intervals = bucket_intervals
        self.calculators: dict[str, VwapTracker] = {}

    def _get_or_create_calculator(self, stock: str) -> VwapTracker:
        calc = self.calculators.get(stock)
        if calc is None:
            calc = VwapTracker(stock, *self.bucket_intervals)
            self.calculators[stock] = calc
        return calc

    def on_trade(self, trade: Trade):
        calc = self._get_or_create_calculator(trade.sec_id)
        calc.add_trade(trade)
        for bucket in calc.buckets.values():
            payload = _serialize_vwap(trade.timestamp_ns, trade.sec_id, bucket)
            self._publish(VWAP_MSG_TYPE, payload)
