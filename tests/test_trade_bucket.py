import datetime
import logging

import pytest
from analytics.TradeBucket import TradeBucket
from book.trade import Trade

TEST_DATE = datetime.date(2024, 1, 15)


def make_trade(timestamp_ns: int, shares: int, price: float,
               side: str = "B", trade_type: str = "E",
               sec_id: str = "AAPL") -> Trade:
    return Trade(timestamp_ns=timestamp_ns, sec_id=sec_id, shares=shares,
                 price=price, side=side, type=trade_type, trade_date=TEST_DATE)


# ---------------------------------------------------------------------------
# Empty state
# ---------------------------------------------------------------------------

class TestTradeBucketEmpty:
    def test_is_empty(self):
        assert TradeBucket(60_000).is_empty

    def test_vwap_is_none(self):
        assert TradeBucket(60_000).vwap() is None

    def test_all_shares_zero(self):
        b = TradeBucket(60_000)
        assert b.total_shares == 0
        assert b.buy_shares == 0
        assert b.sell_shares == 0
        assert b.auction_shares == 0
        assert b.hidden_shares == 0
        assert b.trade_count == 0

    def test_all_volumes_zero(self):
        b = TradeBucket(60_000)
        assert b.notional == 0.0
        assert b.buy_volume == 0.0
        assert b.sell_volume == 0.0
        assert b.auction_volume == 0.0
        assert b.hidden_volume == 0.0


# ---------------------------------------------------------------------------
# Single trade — OHLC and vwap
# ---------------------------------------------------------------------------

class TestTradeBucketSingleTrade:
    def test_not_empty_after_trade(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert not b.is_empty

    def test_ohlc_all_equal_to_price(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.open == 150.0
        assert b.high == 150.0
        assert b.low  == 150.0
        assert b.close == 150.0

    def test_vwap_equals_price(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.vwap() == pytest.approx(150.0)

    def test_notional(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.notional == pytest.approx(15_000.0)

    def test_trade_count(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0))
        assert b.trade_count == 1


# ---------------------------------------------------------------------------
# OHLC tracking
# ---------------------------------------------------------------------------

class TestTradeBucketOHLC:
    def test_high_and_low_tracked(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 120.0))
        b.add_trade(make_trade(2, 10,  80.0))
        assert b.open  == 100.0
        assert b.high  == 120.0
        assert b.low   ==  80.0
        assert b.close ==  80.0

    def test_open_never_changes(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 200.0))
        b.add_trade(make_trade(2, 10,  50.0))
        assert b.open == 100.0

    def test_close_updates_on_each_trade(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 10, 100.0))
        b.add_trade(make_trade(1, 10, 110.0))
        assert b.close == 110.0
        b.add_trade(make_trade(2, 10, 105.0))
        assert b.close == 105.0


# ---------------------------------------------------------------------------
# VWAP weighted by shares
# ---------------------------------------------------------------------------

class TestTradeBucketVwap:
    def test_vwap_weighted_by_shares(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0))   # notional 10_000
        b.add_trade(make_trade(1, 200, 110.0))   # notional 22_000
        assert b.vwap() == pytest.approx(32_000 / 300)

    def test_notional_accumulates(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0,  50, 100.0))
        b.add_trade(make_trade(1,  50, 200.0))
        assert b.notional == pytest.approx(15_000.0)

    def test_total_shares_accumulates(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0))
        b.add_trade(make_trade(1, 200, 110.0))
        assert b.total_shares == 300


# ---------------------------------------------------------------------------
# Buy aggressor (side='B', regular trade type)
# ---------------------------------------------------------------------------

class TestTradeBucketBuyAggressor:
    def test_buy_aggressor_goes_to_buy_shares(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B", trade_type="E"))
        assert b.buy_shares == 100
        assert b.sell_shares == 0
        assert b.auction_shares == 0
        assert b.hidden_shares == 0

    def test_buy_aggressor_volume(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 150.0, side="B", trade_type="E"))
        assert b.buy_volume == pytest.approx(15_000.0)
        assert b.sell_volume == 0.0
        assert b.auction_volume == 0.0
        assert b.hidden_volume == 0.0

    def test_buy_aggressor_with_price_change_type(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 50, 100.0, side="B", trade_type="P"))
        assert b.buy_shares == 50


# ---------------------------------------------------------------------------
# Sell aggressor (side='S')
# ---------------------------------------------------------------------------

class TestTradeBucketSellAggressor:
    def test_sell_aggressor_goes_to_sell_shares(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 200, 100.0, side="S", trade_type="E"))
        assert b.sell_shares == 200
        assert b.buy_shares == 0

    def test_sell_aggressor_volume(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 200, 100.0, side="S", trade_type="E"))
        assert b.sell_volume == pytest.approx(20_000.0)
        assert b.buy_volume == 0.0


# ---------------------------------------------------------------------------
# Auction trades (O, C, H)
# ---------------------------------------------------------------------------

class TestTradeBucketAuction:
    @pytest.mark.parametrize("trade_type", ["O", "C", "H"])
    def test_auction_types_go_to_auction_shares(self, trade_type):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 500, 100.0, side="", trade_type=trade_type))
        assert b.auction_shares == 500
        assert b.buy_shares == 0
        assert b.sell_shares == 0
        assert b.hidden_shares == 0

    @pytest.mark.parametrize("trade_type", ["O", "C", "H"])
    def test_auction_volume(self, trade_type):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 500, 100.0, side="", trade_type=trade_type))
        assert b.auction_volume == pytest.approx(50_000.0)

    def test_auction_with_side_still_goes_to_auction(self):
        """Auction type takes precedence over side indicator."""
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B", trade_type="O"))
        assert b.auction_shares == 100
        assert b.buy_shares == 0


# ---------------------------------------------------------------------------
# Hidden / non-displayable trades (type='N')
# ---------------------------------------------------------------------------

class TestTradeBucketHidden:
    def test_hidden_type_goes_to_hidden_shares(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 300, 100.0, side="B", trade_type="N"))
        assert b.hidden_shares == 300
        assert b.buy_shares == 0
        assert b.auction_shares == 0

    def test_hidden_volume(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 300, 100.0, side="B", trade_type="N"))
        assert b.hidden_volume == pytest.approx(30_000.0)
        assert b.buy_volume == 0.0

    def test_hidden_sell_side_also_classified_as_hidden(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="S", trade_type="N"))
        assert b.hidden_shares == 100
        assert b.sell_shares == 0


# ---------------------------------------------------------------------------
# Unclassified trades — warning logged, excluded from all buckets
# ---------------------------------------------------------------------------

class TestTradeBucketUnclassified:
    def test_unclassified_trade_excluded_from_all_buckets(self):
        b = TradeBucket(60_000)
        # Unknown type, empty side
        b.add_trade(make_trade(0, 100, 100.0, side="", trade_type="U"))
        assert b.buy_shares == 0
        assert b.sell_shares == 0
        assert b.auction_shares == 0
        assert b.hidden_shares == 0

    def test_unclassified_still_counted_in_total_shares_and_notional(self):
        """OHLC, total_shares and notional include all trades for accuracy."""
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="", trade_type="U"))
        assert b.total_shares == 100
        assert b.notional == pytest.approx(10_000.0)
        assert b.trade_count == 1

    def test_unclassified_logs_warning(self, caplog):
        b = TradeBucket(60_000)
        with caplog.at_level(logging.WARNING, logger="analytics.TradeBucket"):
            b.add_trade(make_trade(0, 100, 100.0, side="", trade_type="U"))
        assert any("unhandled" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Invariants: shares and volumes sum to totals
# ---------------------------------------------------------------------------

class TestTradeBucketInvariants:
    def test_shares_sum_to_total(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B", trade_type="E"))
        b.add_trade(make_trade(1, 200, 110.0, side="S", trade_type="E"))
        b.add_trade(make_trade(2, 300, 120.0, side="",  trade_type="O"))
        b.add_trade(make_trade(3,  50, 115.0, side="B", trade_type="N"))
        assert b.total_shares == b.buy_shares + b.sell_shares + b.auction_shares + b.hidden_shares

    def test_volumes_sum_to_notional(self):
        b = TradeBucket(60_000)
        b.add_trade(make_trade(0, 100, 100.0, side="B", trade_type="E"))
        b.add_trade(make_trade(1, 200, 110.0, side="S", trade_type="E"))
        b.add_trade(make_trade(2, 300, 120.0, side="",  trade_type="O"))
        b.add_trade(make_trade(3,  50, 115.0, side="B", trade_type="N"))
        total_vol = b.buy_volume + b.sell_volume + b.auction_volume + b.hidden_volume
        assert total_vol == pytest.approx(b.notional)
