import pytest
from analytics.VwapBucket import VwapBucket
from book.trade import Trade


def make_trade(timestamp_ns: int, shares: int, price: float) -> Trade:
    return Trade(timestamp_ns=timestamp_ns, sec_id="TEST", shares=shares, price=price, side="B", type="regular")


class TestVwapBucketEmpty:
    def test_empty_bucket_returns_none(self):
        bucket = VwapBucket(250)
        assert bucket.vwap_price() is None


class TestVwapBucketSingleTrade:
    def test_vwap_equals_trade_price(self):
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0, 10, 100.0))
        assert bucket.vwap_price() == 100.0
        assert bucket.trade_count == 1


class TestVwapBucketWithinWindow:
    def test_multiple_trades_no_eviction(self):
        """Three trades all within 250ms of the last one — none evicted."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))   # vol = 1000
        bucket.add_trade(make_trade(100_000_000,   20, 110.0))   # vol = 2200
        bucket.add_trade(make_trade(200_000_000,   30, 120.0))   # vol = 3600
        # VWAP = 6800 / 60
        assert bucket.vwap_price() == pytest.approx(6800 / 60)
        assert bucket.trade_count == 3


class TestVwapBucketEviction:
    def test_single_stale_trade_evicted(self):
        """One trade falls outside the 250ms window."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))
        bucket.add_trade(make_trade(300_000_000,   20, 120.0))   # 300ms gap → evicts trade@0
        assert bucket.vwap_price() == 120.0
        assert bucket.trade_count == 1

    def test_multiple_stale_trades_evicted_at_once(self):
        """Gap causes several trades to fall outside — all must be evicted together.

        This is the case that fails if eviction uses `if` instead of `while`:
        only the oldest trade would be removed, leaving stale trades in the window.
        """
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))
        bucket.add_trade(make_trade(50_000_000,    20, 110.0))
        bucket.add_trade(make_trade(100_000_000,   30, 120.0))
        # Jump to 600ms: all three above are > 250ms old relative to 600ms
        bucket.add_trade(make_trade(600_000_000,   40, 130.0))
        assert bucket.vwap_price() == 130.0
        assert bucket.trade_count == 1

    def test_partial_eviction(self):
        """Only the oldest trade is outside the window; the rest stay."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))   # 300ms old → evicted
        bucket.add_trade(make_trade(100_000_000,   20, 110.0))   # 200ms old → stays
        bucket.add_trade(make_trade(200_000_000,   30, 120.0))   # 100ms old → stays
        bucket.add_trade(make_trade(300_000_000,   40, 130.0))   # 0ms   old → stays
        # VWAP = (2200 + 3600 + 5200) / 90
        assert bucket.vwap_price() == pytest.approx(11000 / 90)
        assert bucket.trade_count == 3


class TestVwapBucketBoundary:
    def test_trade_at_exact_boundary_not_evicted(self):
        """Eviction condition is strict <, so a trade exactly interval_ns apart stays."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))
        bucket.add_trade(make_trade(250_000_000,   20, 120.0))   # exactly 250ms
        # Both remain: VWAP = (1000 + 2400) / 30
        assert bucket.vwap_price() == pytest.approx(3400 / 30)
        assert bucket.trade_count == 2

    def test_trade_one_ns_past_boundary_evicted(self):
        """1 ns beyond the boundary triggers eviction."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))
        bucket.add_trade(make_trade(250_000_001,   20, 120.0))
        assert bucket.vwap_price() == 120.0
        assert bucket.trade_count == 1


class TestVwapBucketRollingWindow:
    def test_steady_trades_every_100ms(self):
        """Trades arrive every 100ms; exactly one falls out of the 250ms window per step."""
        bucket = VwapBucket(250)
        bucket.add_trade(make_trade(0,             10, 100.0))
        bucket.add_trade(make_trade(100_000_000,   20, 110.0))
        bucket.add_trade(make_trade(200_000_000,   30, 120.0))
        assert bucket.trade_count == 3

        # t=300ms: trade@0 is 300ms old, evicted
        bucket.add_trade(make_trade(300_000_000,   40, 130.0))
        assert bucket.vwap_price() == pytest.approx((110*20 + 120*30 + 130*40) / 90)
        assert bucket.trade_count == 3

        # t=400ms: trade@100ms is 300ms old, evicted
        bucket.add_trade(make_trade(400_000_000,   50, 140.0))
        assert bucket.vwap_price() == pytest.approx((120*30 + 130*40 + 140*50) / 120)
        assert bucket.trade_count == 3


class TestVwapBucket1sBucket:
    def test_1000ms_no_eviction(self):
        """All trades within 1s of the last — none evicted."""
        bucket = VwapBucket(1000)
        bucket.add_trade(make_trade(0,             10, 50.0))
        bucket.add_trade(make_trade(500_000_000,   10, 60.0))    # 500ms
        bucket.add_trade(make_trade(999_999_999,   10, 70.0))    # just under 1s
        # VWAP = (500 + 600 + 700) / 30 = 60
        assert bucket.vwap_price() == pytest.approx(60.0)
        assert bucket.trade_count == 3

    def test_1000ms_partial_eviction(self):
        """Trade@0 evicted (1.5s old); trade@500ms is exactly 1s old — stays (strict <)."""
        bucket = VwapBucket(1000)
        bucket.add_trade(make_trade(0,             10, 50.0))
        bucket.add_trade(make_trade(500_000_000,   10, 60.0))
        bucket.add_trade(make_trade(1_500_000_000, 10, 70.0))
        # VWAP = (600 + 700) / 20 = 65
        assert bucket.vwap_price() == pytest.approx(65.0)
        assert bucket.trade_count == 2
