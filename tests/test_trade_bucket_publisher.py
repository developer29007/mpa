import datetime
import math
import struct
from unittest.mock import patch

import pytest
from analytics.TradeBucket import TradeBucket
from book.trade import Trade
from publishers.trade_bucket_publisher import (TRADE_BUCKET_FORMAT, TRADE_BUCKET_MSG_TYPE,
                                               TradeBucketPublisher, _serialize_trade_bucket)
from util.TimerService import TimerService

TEST_DATE = datetime.date(2024, 1, 15)
_1MIN_MS = 60_000
_1MIN_NS = _1MIN_MS * 1_000_000


def make_trade(timestamp_ns: int, shares: int, price: float,
               side: str = "B", trade_type: str = "E",
               sec_id: str = "AAPL") -> Trade:
    return Trade(timestamp_ns=timestamp_ns, sec_id=sec_id, shares=shares,
                 price=price, side=side, type=trade_type, trade_date=TEST_DATE)


def unpack_tb(data: bytes):
    return struct.unpack(TRADE_BUCKET_FORMAT, data)


@pytest.fixture(autouse=True)
def reset_timer_service():
    TimerService._instance = None
    yield
    TimerService._instance = None


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

class TestTradeBucketSerializationSize:
    def test_payload_size_matches_format(self):
        b = TradeBucket(_1MIN_MS, bucket_start_ns=0)
        b.add_trade(make_trade(0, 100, 150.0))
        data = _serialize_trade_bucket("AAPL", b)
        assert len(data) == struct.calcsize(TRADE_BUCKET_FORMAT)


class TestTradeBucketSerializationRoundTrip:
    def test_all_fields_round_trip(self):
        BUCKET_START = 1_000_000_000
        b = TradeBucket(_1MIN_MS, bucket_start_ns=BUCKET_START)
        b.add_trade(make_trade(0,  100, 150.0, side="B", trade_type="E"))   # buy
        b.add_trade(make_trade(1,   50, 160.0, side="S", trade_type="E"))   # sell
        b.add_trade(make_trade(2,  200, 140.0, side="",  trade_type="O"))   # auction
        b.add_trade(make_trade(3,   25, 155.0, side="B", trade_type="N"))   # hidden

        data = _serialize_trade_bucket("AAPL", b)
        (msg_id, ts, stock, interval_ms,
         open_, high, low, close, notional, vwap,
         total_shares, buy_shares, sell_shares, auction_shares, hidden_shares, trade_count,
         buy_volume, sell_volume, auction_volume, hidden_volume) = unpack_tb(data)

        assert ts           == BUCKET_START
        assert stock        == b'AAPL    '
        assert interval_ms  == _1MIN_MS
        assert open_        == pytest.approx(150.0)
        assert high         == pytest.approx(160.0)
        assert low          == pytest.approx(140.0)
        assert close        == pytest.approx(155.0)
        assert total_shares == 375
        assert buy_shares   == 100
        assert sell_shares  ==  50
        assert auction_shares == 200
        assert hidden_shares  ==  25
        assert trade_count  == 4
        expected_notional = 100*150 + 50*160 + 200*140 + 25*155
        assert notional     == pytest.approx(expected_notional)
        assert vwap         == pytest.approx(expected_notional / 375)
        assert buy_volume   == pytest.approx(100 * 150.0)
        assert sell_volume  == pytest.approx(50  * 160.0)
        assert auction_volume == pytest.approx(200 * 140.0)
        assert hidden_volume  == pytest.approx(25  * 155.0)

    def test_msg_id_is_positive(self):
        b = TradeBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        msg_id, *_ = unpack_tb(_serialize_trade_bucket("AAPL", b))
        assert msg_id > 0

    def test_msg_ids_are_unique(self):
        b = TradeBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        id1, *_ = unpack_tb(_serialize_trade_bucket("AAPL", b))
        id2, *_ = unpack_tb(_serialize_trade_bucket("AAPL", b))
        assert id1 != id2

    def test_stock_padded_to_8_bytes(self):
        b = TradeBucket(_1MIN_MS)
        b.add_trade(make_trade(0, 10, 100.0))
        _, _, stock, *_ = unpack_tb(_serialize_trade_bucket("T", b))
        assert stock == b'T       '

    def test_msg_type_is_K(self):
        assert TRADE_BUCKET_MSG_TYPE == 'K'

    def test_empty_bucket_nan_vwap(self):
        b = TradeBucket(_1MIN_MS, bucket_start_ns=0)
        b.add_trade(make_trade(0, 10, 100.0))
        data = _serialize_trade_bucket("AAPL", b)
        _, _, _, _, _, _, _, _, _, vwap, *_ = unpack_tb(data)
        assert not math.isnan(vwap)   # non-empty bucket → real vwap

    def test_all_volumes_sum_to_notional(self):
        b = TradeBucket(_1MIN_MS, bucket_start_ns=0)
        b.add_trade(make_trade(0, 100, 150.0, side="B", trade_type="E"))
        b.add_trade(make_trade(1,  50, 160.0, side="S", trade_type="E"))
        b.add_trade(make_trade(2, 200, 140.0, side="",  trade_type="O"))
        b.add_trade(make_trade(3,  25, 155.0, side="B", trade_type="N"))
        data = _serialize_trade_bucket("AAPL", b)
        (_, _, _, _, _, _, _, _, notional, _,
         _, _, _, _, _, _,
         buy_vol, sell_vol, auction_vol, hidden_vol) = unpack_tb(data)
        assert buy_vol + sell_vol + auction_vol + hidden_vol == pytest.approx(notional)


# ---------------------------------------------------------------------------
# Publisher — bucket boundary logic
# ---------------------------------------------------------------------------

class TestTradeBucketPublisherBoundary:
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_trades_in_same_bucket_not_published(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.on_trade(make_trade(30_000_000_000, 200, 110.0))
        mock_publish.assert_not_called()

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_crossing_boundary_publishes_previous_bucket(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.on_trade(make_trade(70_000_000_000, 200, 110.0))
        assert mock_publish.call_count == 1

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_crossing_two_boundaries_publishes_twice(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000,  100, 100.0))
        pub.on_trade(make_trade(70_000_000_000,  200, 110.0))
        pub.on_trade(make_trade(130_000_000_000, 300, 120.0))
        assert mock_publish.call_count == 2

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_two_stocks_track_independently(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 100, 200.0, sec_id="TSLA"))
        pub.on_trade(make_trade(70_000_000_000, 100, 105.0, sec_id="AAPL"))
        assert mock_publish.call_count == 1  # only AAPL crossed boundary

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_published_payload_has_correct_ohlc(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.on_trade(make_trade(20_000_000_000, 100, 120.0))
        pub.on_trade(make_trade(30_000_000_000, 100,  90.0))
        pub.on_trade(make_trade(70_000_000_000, 100, 200.0))  # triggers publish

        _, payload = mock_publish.call_args[0]
        _, _, _, _, open_, high, low, close, *_ = unpack_tb(payload)
        assert open_  == pytest.approx(100.0)
        assert high   == pytest.approx(120.0)
        assert low    == pytest.approx(90.0)
        assert close  == pytest.approx(90.0)

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_published_payload_has_correct_side_breakdown(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, side="B", trade_type="E"))  # buy
        pub.on_trade(make_trade(20_000_000_000,  50, 100.0, side="S", trade_type="E"))  # sell
        pub.on_trade(make_trade(30_000_000_000, 200, 100.0, side="",  trade_type="O"))  # auction
        pub.on_trade(make_trade(70_000_000_000, 100, 200.0))  # triggers publish

        _, payload = mock_publish.call_args[0]
        (_, _, _, _, _, _, _, _, _,  _,
         total_shares, buy_shares, sell_shares, auction_shares, hidden_shares, trade_count,
         buy_vol, sell_vol, auction_vol, hidden_vol) = unpack_tb(payload)
        assert total_shares   == 350
        assert buy_shares     == 100
        assert sell_shares    ==  50
        assert auction_shares == 200
        assert hidden_shares  ==   0
        assert trade_count    ==   3


# ---------------------------------------------------------------------------
# Publisher — timer behaviour
# ---------------------------------------------------------------------------

class TestTradeBucketPublisherTimer:
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_timer_registered_on_first_trade(self, _mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        assert pub._timer_registered
        assert TimerService.instance().timers

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_timer_fires_and_publishes_open_bucket(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        mock_publish.assert_not_called()
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 1

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_timer_does_not_double_publish(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.on_trade(make_trade(70_000_000_000, 100, 110.0))  # publishes bucket 0
        assert mock_publish.call_count == 1
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 1  # no double-publish

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_timer_publishes_all_stocks(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 200, 200.0, sec_id="TSLA"))
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 2

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    def test_timer_re_registers_for_next_boundary(self, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        TimerService.instance().check_timers(60_000_000_000)
        assert mock_publish.call_count == 1
        pub.on_trade(make_trade(90_000_000_000, 100, 110.0))
        TimerService.instance().check_timers(120_000_000_000)
        assert mock_publish.call_count == 2


# ---------------------------------------------------------------------------
# Publisher — flush
# ---------------------------------------------------------------------------

class TestTradeBucketPublisherFlush:
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.flush')
    def test_flush_publishes_open_bucket(self, mock_kflush, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0))
        pub.flush()
        assert mock_publish.call_count == 1
        mock_kflush.assert_called_once()

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.flush')
    def test_flush_publishes_all_stocks(self, _mock_kflush, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.on_trade(make_trade(10_000_000_000, 100, 100.0, sec_id="AAPL"))
        pub.on_trade(make_trade(10_000_000_000, 200, 200.0, sec_id="TSLA"))
        pub.flush()
        assert mock_publish.call_count == 2

    @patch('publishers.trade_bucket_publisher.KafkaPublisher.__init__', return_value=None)
    @patch('publishers.trade_bucket_publisher.KafkaPublisher._publish')
    @patch('publishers.trade_bucket_publisher.KafkaPublisher.flush')
    def test_flush_with_no_trades_does_not_publish(self, _mock_kflush, mock_publish, _mock_init):
        pub = TradeBucketPublisher('localhost:9092', 'tradebucket', _1MIN_MS)
        pub.flush()
        mock_publish.assert_not_called()
