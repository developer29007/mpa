import logging

from book.trade import Trade

_log = logging.getLogger(__name__)

# Trade types that represent auction/cross executions
_AUCTION_TYPES = frozenset(('O', 'C', 'H'))  # Opening, Closing, IPO/Halt cross

# Trade type for non-displayable (midpoint / dark) executions
_HIDDEN_TYPE = 'N'


class TradeBucket:
    """Aggregates trades within a fixed time interval into OHLC, VWAP, and
    side-broken-down share/notional counters.

    Terminology:
      notional    — Σ(price × shares) across all trades in the bucket
      buy_shares  — shares where the buyer was the aggressor (lifted the offer)
      sell_shares — shares where the seller was the aggressor (hit the bid)
      auction_shares — shares from opening/closing/IPO/halt cross executions
      hidden_shares  — shares from non-displayable (midpoint/dark) executions
      buy_volume / sell_volume / auction_volume / hidden_volume — corresponding notional
    """

    def __init__(self, interval_ms: int, bucket_start_ns: int = 0):
        self.interval_ms: int = interval_ms
        self.bucket_start_ns: int = bucket_start_ns
        self.open: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.close: float | None = None
        self.notional: float = 0.0
        self.total_shares: int = 0
        self.buy_shares: int = 0
        self.sell_shares: int = 0
        self.auction_shares: int = 0
        self.hidden_shares: int = 0
        self.trade_count: int = 0
        self.buy_volume: float = 0.0
        self.sell_volume: float = 0.0
        self.auction_volume: float = 0.0
        self.hidden_volume: float = 0.0

    @property
    def is_empty(self) -> bool:
        return self.open is None

    def add_trade(self, trade: Trade) -> None:
        p = trade.price
        if self.open is None:
            self.open = self.high = self.low = p
        else:
            if p > self.high:
                self.high = p
            if p < self.low:
                self.low = p
        self.close = p

        value = p * trade.shares
        self.notional += value
        self.total_shares += trade.shares
        self.trade_count += 1

        if trade.type in _AUCTION_TYPES:
            self.auction_shares += trade.shares
            self.auction_volume += value
        elif trade.type == _HIDDEN_TYPE:
            self.hidden_shares += trade.shares
            self.hidden_volume += value
        else:
            side = (trade.side or "").strip()
            if side == 'B':        # buy aggressor lifted the offer
                self.buy_shares += trade.shares
                self.buy_volume += value
            elif side == 'S':      # sell aggressor hit the bid
                self.sell_shares += trade.shares
                self.sell_volume += value
            else:
                _log.warning("TradeBucket: unhandled trade type=%r side=%r", trade.type, trade.side)

    def vwap(self) -> float | None:
        if self.total_shares == 0:
            return None
        return self.notional / self.total_shares
