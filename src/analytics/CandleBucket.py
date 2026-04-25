from book.trade import Trade


class CandleBucket:

    def __init__(self, interval_ms: int, bucket_start_ns: int = 0):
        self.interval_ms: int = interval_ms
        self.bucket_start_ns: int = bucket_start_ns
        self.open: float | None = None
        self.high: float | None = None
        self.low: float | None = None
        self.close: float | None = None
        self.dollar_volume: float = 0.0
        self.total_vol: int = 0
        self.bid_vol: int = 0
        self.offer_vol: int = 0
        self.auction_vol: int = 0
        self.trade_count: int = 0

    @property
    def is_empty(self) -> bool:
        return self.open is None

    def add_trade(self, trade: Trade):
        p = trade.price
        if self.open is None:
            self.open = self.high = self.low = p
        else:
            if p > self.high:
                self.high = p
            if p < self.low:
                self.low = p
        self.close = p
        self.dollar_volume += p * trade.shares
        self.total_vol += trade.shares
        side = (trade.side or "").strip()
        if side == "B":
            self.offer_vol += trade.shares   # buy aggressor lifted the offer
        elif side == "S":
            self.bid_vol += trade.shares     # sell aggressor hit the bid
        else:
            self.auction_vol += trade.shares
        self.trade_count += 1

    def vwap(self) -> float | None:
        if self.total_vol == 0:
            return None
        return self.dollar_volume / self.total_vol
