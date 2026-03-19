from collections import deque
from typing import Optional

from book.trade import Trade
from util.TimeUtil import ms_to_nanos


class VwapBucket:

    def __init__(self, interval_ms: int):
        self.interval_ms: int = interval_ms
        self.interval_ns: int = ms_to_nanos(interval_ms)
        # track trades for this bucket interval
        self.trades = deque()
        self.last_trade_time = None
        self.trade_count: int = 0
        self.last_trade: Optional[float] = None
        self.volume_traded: float = 0
        self.shares_traded: int = 0

    def add_trade(self, trade: Trade):
        """Add a trade to the bucket and evict trades outside the rolling window.

        Args:
            trade (Trade): The trade to add, containing price, shares, and timestamp_ns.
        """
        self.trades.append(trade)
        self.trade_count += 1
        self.last_trade = trade.price
        self.last_trade_time = trade.timestamp_ns
        self.shares_traded += trade.shares
        self.volume_traded += (trade.shares * trade.price)

        while self.trades and self.trades[0].timestamp_ns + self.interval_ns < trade.timestamp_ns:
            old_trade: Trade = self.trades.popleft()
            self.trade_count -= 1
            self.shares_traded -= old_trade.shares
            self.volume_traded -= (old_trade.shares * old_trade.price)

    def vwap_price(self):
        if self.trade_count == 0:
            return None
        return self.volume_traded / self.shares_traded
