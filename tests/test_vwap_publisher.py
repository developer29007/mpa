import datetime
import math
import struct
from unittest.mock import patch, MagicMock

from analytics.VwapBucket import VwapBucket
from book.trade import Trade
from publishers.vwap_publisher import _serialize_vwap, VWAP_FORMAT, VwapPublisher


def _unpack_vwap(data: bytes):
    return struct.unpack(VWAP_FORMAT, data)


class TestVwapSerializationRoundTrip:
    def test_bucket_with_trades(self):
        bucket = VwapBucket(250)
        trade = Trade(timestamp_ns=1_000_000, sec_id="AAPL", shares=100, price=150.0, side="B", type="E", trade_date=datetime.date(2024, 1, 15))
        bucket.add_trade(trade)

        data = _serialize_vwap(1_000_000, "AAPL", bucket)
        ts, stock, interval_ms, vwap, volume, shares, count = _unpack_vwap(data)

        assert ts == 1_000_000
        assert stock == b'AAPL    '
        assert interval_ms == 250
        assert abs(vwap - 150.0) < 1e-9
        assert abs(volume - 15000.0) < 1e-9
        assert shares == 100
        assert count == 1

    def test_empty_bucket_nan_vwap(self):
        bucket = VwapBucket(1000)

        data = _serialize_vwap(0, "TEST", bucket)
        _, _, _, vwap, volume, shares, count = _unpack_vwap(data)

        assert math.isnan(vwap)
        assert abs(volume - 0.0) < 1e-9
        assert shares == 0
        assert count == 0


class TestVwapSerializationSize:
    def test_payload_is_44_bytes(self):
        bucket = VwapBucket(250)
        data = _serialize_vwap(0, "X", bucket)
        assert len(data) == struct.calcsize(VWAP_FORMAT)


class TestVwapPublisherIntegration:
    @patch('publishers.vwap_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.vwap_publisher.KafkaPublisher._publish')
    def test_on_trade_creates_calculator_and_publishes(self, mock_publish, mock_init):
        publisher = VwapPublisher(
            bootstrap_servers='localhost:9092',
            trade_date=datetime.date(2024, 1, 15),
            bucket_intervals=[250, 1000, 5000],
        )

        trade = Trade(timestamp_ns=1_000_000, sec_id="AAPL", shares=100, price=150.0, side="B", type="E", trade_date=datetime.date(2024, 1, 15))
        publisher.on_trade(trade)

        assert "AAPL" in publisher.calculators
        assert mock_publish.call_count == 3  # one per bucket

    @patch('publishers.vwap_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.vwap_publisher.KafkaPublisher._publish')
    def test_multiple_stocks_separate_calculators(self, mock_publish, mock_init):
        publisher = VwapPublisher(
            bootstrap_servers='localhost:9092',
            trade_date=datetime.date(2024, 1, 15),
            bucket_intervals=[250, 1000],
        )

        trade_aapl = Trade(timestamp_ns=1_000_000, sec_id="AAPL", shares=100, price=150.0, side="B", type="E", trade_date=datetime.date(2024, 1, 15))
        trade_tsla = Trade(timestamp_ns=2_000_000, sec_id="TSLA", shares=50, price=200.0, side="S", type="E", trade_date=datetime.date(2024, 1, 15))
        publisher.on_trade(trade_aapl)
        publisher.on_trade(trade_tsla)

        assert len(publisher.calculators) == 2
        assert mock_publish.call_count == 4  # 2 buckets x 2 stocks
