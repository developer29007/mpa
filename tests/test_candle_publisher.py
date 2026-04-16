import datetime
import math
import struct
from unittest.mock import patch

import pytest
from analytics.CandleBucket import CandleBucket
from book.trade import Trade
from publishers.candle_publisher import CANDLE_FORMAT, CandlePublisher, _serialize_candle
from util.TimerService import TimerService

TEST_DATE = datetime.date(2024, 1, 15)

# 1-minute interval expressed in nanoseconds: 60 000 ms * 1 000 000 ns/ms
_1MIN_MS = 60_000
_1MIN_NS = _1MIN_MS * 1_000_000


def make_trade(timestamp_ns: int, shares: int, price: float,
               side: str = "B", sec_id: str = "AAPL") -> Trade:
    return Trade(timestamp_ns=timestamp_ns, sec_id=sec_id, shares=shares, price=price,
                 side=side, type="E", trade_date=TEST_DATE)


def unpack_candle(data: bytes):
    return struct.unpack(CANDLE_FORMAT, data)


@pytest.fixture(autouse=True)
def reset_timer_service():
    """Reset the TimerService singleton before and after each test."""
    TimerService._instance = None
    yield
    TimerService._instance = None


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

class TestCandleSerializationSize:
    def test_payload_is_correct_size(self):
        b = CandleBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 100, 150.0))
        data = _serialize_candle(0, "AAPL", b)
        assert len(data) == struct.calcsize(CANDLE_FORMAT)


class TestCandleSerializationRoundTrip:
    def test_all_fields_round_trip(self):
        b = CandleBucket(_1MIN_MS)
        b.add_trade(make_trade(0,  100, 150.0, side="B"))   # offer_vol += 100
        b.add_trade(make_trade(1,   50, 160.0, side="S"))   # bid_vol   +=  50
        b.add_trade(make_trade(2,   25,  80.0, side=""))    # auction_vol+= 25

        BUCKET_START = 1_000_000_000
        data = _serialize_candle(BUCKET_START, "AAPL", b)
        (msg_id, ts, stock, interval_ms,
         open_, high, low, close,
         dollar_vol, vwap,
         total_vol, bid_vol, offer_vol, auction_vol, trade_count) = unpack_candle(data)

        assert ts          == BUCKET_START
        assert stock       == b'AAPL    '
        assert interval_ms == _1MIN_MS
        assert open_       == pytest.approx(150.0)
        assert high        == pytest.approx(160.0)
        assert low         == pytest.approx(80.0)
        assert close       == pytest.approx(80.0)
        expected_dollar = 100 * 150.0 + 50 * 160.0 + 25 * 80.0
        assert dollar_vol  == pytest.approx(expected_dollar)
        assert vwap        == pytest.approx(expected_dollar / 175)
        assert total_vol   == 175
        assert offer_vol   == 100
        assert bid_vol     ==  50
        assert auction_vol ==  25
        assert trade_count ==   3

    def test_msg_id_is_positive(self):
        b = CandleBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        (msg_id, *_) = unpack_candle(_serialize_candle(0, "AAPL", b))
        assert msg_id > 0

    def test_msg_ids_are_unique(self):
        b = CandleBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        id1, *_ = unpack_candle(_serialize_candle(0, "AAPL", b))
        id2, *_ = unpack_candle(_serialize_candle(0, "AAPL", b))
        assert id1 != id2

    def test_stock_symbol_padded_to_8_bytes(self):
        b = CandleBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        _, _, stock, *_ = unpack_candle(_serialize_candle(0, "T", b))
        assert stock == b'T       '


# ---------------------------------------------------------------------------
# Publisher — bucket boundary logic (on_trade)
# ---------------------------------------------------------------------------

class TestCandlePublisherBucketBoundary:
    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_trades_in_same_bucket_not_published(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        # Both trades fall in the same 1-min bucket (0–60 000 ms)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))   # t = 10 s
        pub.on_trade(make_trade(30_000_000_000, 200, 110.0))   # t = 30 s
        mock_publish.assert_not_called()

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_crossing_boundary_publishes_previous_bucket(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))   # bucket 0
        pub.on_trade(make_trade(70_000_000_000, 200, 110.0))   # bucket 1 → publish bucket 0
        assert mock_publish.call_count == 1

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_crossing_two_boundaries_publishes_once_per_crossing(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000,  100, 100.0))  # bucket 0
        pub.on_trade(make_trade(70_000_000_000,  200, 110.0))  # bucket 1 → publish bucket 0
        pub.on_trade(make_trade(130_000_000_000, 300, 120.0))  # bucket 2 → publish bucket 1
        assert mock_publish.call_count == 2

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_two_stocks_track_independently(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 100, 200.0, sec_id="TSLA"))
        # Cross boundary for AAPL only
        pub.on_trade(make_trade(70_000_000_000, 100, 105.0, sec_id="AAPL"))
        assert mock_publish.call_count == 1   # only AAPL's bucket 0

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_published_candle_contains_correct_ohlc(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))   # open
        pub.on_trade(make_trade(20_000_000_000, 100, 120.0))   # high
        pub.on_trade(make_trade(30_000_000_000, 100,  90.0))   # low + close
        pub.on_trade(make_trade(70_000_000_000, 100, 200.0))   # triggers publish

        assert mock_publish.call_count == 1
        _, payload = mock_publish.call_args[0]   # (msg_type, raw_struct_bytes)
        _, _, _, _, open_, high, low, close, *_ = unpack_candle(payload)
        assert open_  == pytest.approx(100.0)
        assert high   == pytest.approx(120.0)
        assert low    == pytest.approx(90.0)
        assert close  == pytest.approx(90.0)


# ---------------------------------------------------------------------------
# Publisher — timer behaviour
# ---------------------------------------------------------------------------

class TestCandlePublisherTimer:
    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_registered_on_first_trade(self, _mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        assert pub._timer_registered
        assert TimerService.instance().timers  # a timer is queued

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_not_registered_twice(self, _mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.on_trade(make_trade(20_000_000_000, 100, 110.0))
        assert len(TimerService.instance().timers) == 1

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_fires_and_publishes_open_bucket(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))  # bucket starts at t=0
        mock_publish.assert_not_called()

        # Advance market time past the 1-min boundary (60 s = 60 000 000 000 ns)
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 1

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_does_not_double_publish_after_on_trade_boundary_crossing(self, mock_publish, _mock_init):
        """on_trade already published bucket 0 when a trade crossed the boundary.
        The timer firing for the same boundary should not publish again."""
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))   # bucket 0
        pub.on_trade(make_trade(70_000_000_000, 100, 110.0))   # crosses → publishes bucket 0
        assert mock_publish.call_count == 1

        # Timer fires for the 60 s boundary — bucket 0 is already gone
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 1   # still just 1

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_publishes_all_stocks_at_boundary(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 200, 200.0, sec_id="TSLA"))
        # No new trades — both buckets still open
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 2  # one per stock

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    def test_timer_re_registers_for_next_boundary(self, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        TimerService.instance().check_timers(60_000_000_000)   # fires at 60 s
        assert mock_publish.call_count == 1

        pub.on_trade(make_trade(90_000_000_000, 100, 110.0))   # trade in bucket 1
        TimerService.instance().check_timers(120_000_000_000)  # fires at 120 s
        assert mock_publish.call_count == 2  # bucket 1 published by timer


# ---------------------------------------------------------------------------
# Publisher — flush
# ---------------------------------------------------------------------------

class TestCandlePublisherFlush:
    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    @patch('publishers.candle_publisher.KafkaPublisher.flush')
    def test_flush_publishes_open_bucket(self, mock_kflush, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.flush()
        assert mock_publish.call_count == 1
        mock_kflush.assert_called_once()

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    @patch('publishers.candle_publisher.KafkaPublisher.flush')
    def test_flush_publishes_all_stocks(self, _mock_kflush, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 200, 200.0, sec_id="TSLA"))
        pub.flush()
        assert mock_publish.call_count == 2

    @patch('publishers.candle_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.candle_publisher.KafkaPublisher._publish')
    @patch('publishers.candle_publisher.KafkaPublisher.flush')
    def test_flush_with_no_trades_does_not_publish(self, _mock_kflush, mock_publish, _mock_init):
        pub = CandlePublisher('localhost:9092', 'candles', _1MIN_MS)
        pub.flush()
        mock_publish.assert_not_called()
