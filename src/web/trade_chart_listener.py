import json
import calendar

from book.trade import Trade
from book.trade_listener import TradeListener
from web.chart_server import ChartServer


def _trade_to_json(trade: Trade) -> str:
    """Convert a Trade to compact JSON for WebSocket broadcast."""
    epoch_seconds = (
        calendar.timegm(trade.trade_date.timetuple())
        + trade.timestamp_ns / 1_000_000_000
    )
    return json.dumps({
        "s": trade.sec_id.strip(),
        "p": trade.price,
        "t": epoch_seconds,
        "v": trade.shares,
        "sd": trade.side.strip() if trade.side else "",
    }, separators=(",", ":"))


class TradeChartListener(TradeListener):

    def __init__(self, stocks: set[str] | None = None, host: str = "localhost", port: int = 8765):
        self.stocks = stocks
        self.server = ChartServer(host=host, port=port)
        self.server.start_background()

    def on_trade(self, trade: Trade):
        if self.stocks is not None and trade.sec_id not in self.stocks:
            return
        self.server.enqueue_trade(_trade_to_json(trade))
