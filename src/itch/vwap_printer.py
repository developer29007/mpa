from analytics.VwapCalculator import VwapCalculator
from book.trade import Trade
from book.trade_listener import TradeListener
from util.TimeUtil import nanos_to_ms_str


class VwapPrinter(TradeListener):

    def __init__(self, stocks: set[str], bucket_intervals: list[int]):
        self.stocks = stocks
        self.bucket_intervals = bucket_intervals
        self.calculators: dict[str, VwapCalculator] = {}

    def _get_or_create_calculator(self, stock: str) -> VwapCalculator:
        calc = self.calculators.get(stock)
        if calc is None:
            calc = VwapCalculator(stock, *self.bucket_intervals)
            self.calculators[stock] = calc
        return calc

    def on_trade(self, trade: Trade):
        if trade.sec_id not in self.stocks:
            return
        calc = self._get_or_create_calculator(trade.sec_id)
        calc.add_trade(trade)
        time_str = nanos_to_ms_str(trade.timestamp_ns)
        for interval, bucket in calc.buckets.items():
            vwap = bucket.vwap_price()
            if vwap is not None:
                print(f"{time_str} VWAP {trade.sec_id} {interval}ms vwap={vwap:.4f} vol={bucket.volume_traded:.2f} shares={bucket.shares_traded} count={bucket.trade_count}")
