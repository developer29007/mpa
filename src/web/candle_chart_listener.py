import calendar
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from book.trade import Trade
from book.trade_listener import TradeListener
from web.chart_server import ChartServer


@dataclass
class _Candle:
    time: int       # bucket start (epoch seconds)
    open: float
    high: float
    low: float
    close: float
    total_vol: int = 0
    bid_vol: int = 0
    offer_vol: int = 0
    auction_vol: int = 0

    def to_json(self, symbol: str) -> str:
        return json.dumps({
            "s": symbol,
            "t": self.time,
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "tv": self.total_vol,
            "bv": self.bid_vol,
            "ov": self.offer_vol,
            "av": self.auction_vol,
        }, separators=(",", ":"))


class CandleChartListener(TradeListener):
    """
    Aggregates trades into OHLCV candles with bid/offer volume split and
    streams candle updates to a WebSocket chart server.

    Traffic reduction: at most one candle update per stock is sent per flush
    interval (default 50ms), regardless of how many trades arrive in that window.
    """

    _FLUSH_INTERVAL = 0.05  # 50ms

    def __init__(
        self,
        stocks: set[str] | None = None,
        host: str = "localhost",
        port: int = 8766,
        interval_seconds: int = 60,
    ):
        self.stocks = stocks
        self.interval_seconds = interval_seconds

        html_file = str(Path(__file__).parent / "candle_chart.html")
        self.server = ChartServer(host=host, port=port, html_file=html_file)
        self.server.start_background()

        self._candles: dict[str, _Candle] = {}   # symbol -> current candle
        self._dirty: set[str] = set()             # symbols updated since last flush
        self._lock = threading.Lock()

        flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        flush_thread.start()

    def _bucket_ts(self, epoch_seconds: float) -> int:
        return int(epoch_seconds // self.interval_seconds) * self.interval_seconds

    def on_trade(self, trade: Trade):
        symbol = trade.sec_id.strip()
        if self.stocks is not None and symbol not in self.stocks:
            return

        epoch_seconds = (
            calendar.timegm(trade.trade_date.timetuple())
            + trade.timestamp_ns / 1_000_000_000
        )
        bucket_ts = self._bucket_ts(epoch_seconds)

        with self._lock:
            candle = self._candles.get(symbol)
            if candle is None or candle.time != bucket_ts:
                candle = _Candle(
                    time=bucket_ts,
                    open=trade.price,
                    high=trade.price,
                    low=trade.price,
                    close=trade.price,
                )
                self._candles[symbol] = candle
            else:
                candle.high  = max(candle.high, trade.price)
                candle.low   = min(candle.low,  trade.price)
                candle.close = trade.price

            candle.total_vol += trade.shares
            side = (trade.side or "").strip()
            if side == "B":
                candle.offer_vol += trade.shares   # buy aggressor lifted the offer
            elif side == "S":
                candle.bid_vol += trade.shares     # sell aggressor hit the bid
            else:
                candle.auction_vol += trade.shares  # cross/auction — no side

            self._dirty.add(symbol)

    def _flush_loop(self):
        while True:
            time.sleep(self._FLUSH_INTERVAL)
            with self._lock:
                if not self._dirty:
                    continue
                to_send = [(s, self._candles[s].to_json(s)) for s in self._dirty if s in self._candles]
                self._dirty.clear()

            for _, candle_json in to_send:
                self.server.enqueue_trade(candle_json)
