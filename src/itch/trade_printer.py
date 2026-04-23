from typing import Optional

from book.trade import Trade
from book.trade_listener import TradeListener
from util.TimeUtil import nanos_to_us_str


class TradePrinter(TradeListener):

    def __init__(self, stocks: Optional[set[str]]):
        self.stocks = stocks

    def on_trade(self, trade: Trade):
        if self.stocks is not None and trade.sec_id not in self.stocks:
            return
        time_str = nanos_to_us_str(trade.timestamp_ns)
        print(f"{time_str} TRADE {trade.sec_id} {trade.shares} @ {trade.price} {trade.side_label} {trade.type_label}")
