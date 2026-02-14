from book.trade import Trade
from book.trade_listener import TradeListener
from util.TimeUtil import nanos_to_ms_str


class TradePrinter(TradeListener):

    def __init__(self, stocks: set[str]):
        self.stocks = stocks

    def on_trade(self, trade: Trade):
        if trade.sec_id not in self.stocks:
            return
        time_str = nanos_to_ms_str(trade.timestamp_ns)
        print(f"{time_str} TRADE {trade.sec_id} {trade.shares}@{trade.price} {trade.side} {trade.type}")
