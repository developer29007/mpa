import datetime

import pytest
from analytics.CandleBucket import CandleBucket
from book.trade import Trade

TEST_DATE = datetime.date(2024, 1, 15)


def make_trade(timestamp_ns: int, shares: int, price: float, side: str = "B", sec_id: str = "AAPL") -> Trade:
    return Trade(timestamp_ns=timestamp_ns, sec_id=sec_id, shares=shares, price=price,
                 side=side, type="E", trade_date=TEST_DATE)


class TestCandleBucketEmpty:
    def test_is_empty(self):
        assert CandleBucket(60_000).is_empty

    def test_vwap_is_none(self):
        assert CandleBucket(60_000).vwap() is None

    def test_all_volumes_zero(self):
        b = CandleBucket(60_000)
        assert b.total_vol == 0
        assert b.bid_vol == 0
        assert b.offer_vol == 0
        assert b.auction_vol == 0
        assert b.trade_count == 0
        assert b.dollar_volume == 0.0


class TestCandleBucketSingleTrade:
    def test_ohlc_all_equal_to_price(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.open == 150.0
        assert b.high == 150.0
        assert b.low == 150.0
        assert b.close == 150.0

    def test_not_empty_after_trade(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert not b.is_empty

    def test_vwap_equals_price(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.vwap() == pytest.approx(150.0)

    def test_dollar_volume(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.dollar_volume == pytest.approx(15_000.0)

    def test_trade_count(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.trade_count == 1


class TestCandleBucketOHLC:
    def test_high_and_low_tracked(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 120.0))
        b.add_trade(make_trade(2, 10,  90.0))
        assert b.open  == 100.0
        assert b.high  == 120.0
        assert b.low   ==  90.0
        assert b.close ==  90.0

    def test_close_updates_on_every_trade(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 110.0))
        assert b.close == 110.0
        b.add_trade(make_trade(2, 10, 105.0))
        assert b.close == 105.0

    def test_open_never_changes(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 200.0))
        b.add_trade(make_trade(2, 10,  50.0))
        assert b.open == 100.0


class TestCandleBucketVwap:
    def test_vwap_weighted_by_shares(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0))  # $10 000
        b.add_trade(make_trade(1, 200, 110.0))  # $22 000
        # vwap = 32 000 / 300
        assert b.vwap() == pytest.approx(32_000 / 300)

    def test_dollar_volume_accumulates(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0,  50, 100.0))   # $5 000
        b.add_trade(make_trade(1,  50, 200.0))   # $10 000
        assert b.dollar_volume == pytest.approx(15_000.0)

    def test_total_vol_accumulates(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0))
        b.add_trade(make_trade(1, 200, 110.0))
        assert b.total_vol == 300


class TestCandleBucketSideHandling:
    def test_buy_aggressor_goes_to_offer_vol(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B"))
        assert b.offer_vol == 100
        assert b.bid_vol == 0
        assert b.auction_vol == 0

    def test_sell_aggressor_goes_to_bid_vol(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="S"))
        assert b.bid_vol == 100
        assert b.offer_vol == 0
        assert b.auction_vol == 0

    def test_empty_side_goes_to_auction_vol(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side=""))
        assert b.auction_vol == 100
        assert b.bid_vol == 0
        assert b.offer_vol == 0

    def test_whitespace_side_goes_to_auction_vol(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side=" "))
        assert b.auction_vol == 100

    def test_mixed_sides_split_correctly(self):
        b = CandleBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B"))
        b.add_trade(make_trade(1, 200, 100.0, side="S"))
        b.add_trade(make_trade(2,  50, 100.0, side=""))
        assert b.total_vol   == 350
        assert b.offer_vol   == 100
        assert b.bid_vol     == 200
        assert b.auction_vol ==  50
