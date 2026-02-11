import datetime
from typing import Optional

from sortedcontainers import SortedDict

from book.tob_listener import TobListener
from book.top_of_book import TopOfBook
from book.order import Order
from book.price_level import PriceLevel
from book.trade import Trade
from book.trade_listener import TradeListener
from itertools import zip_longest

from util import TimeUtil
from util.TimeUtil import nanos_to_ms_str

'''
OrderBook for a given stock.
'''


class OrderBook:

    def __init__(self, stock):
        self.stock = stock
        self.timestamp_ns: int = 0
        self.top_of_book = TopOfBook(stock)
        self.bids = SortedDict()
        self.asks = SortedDict()
        self.trade_listeners: [TradeListener] = []
        self.tob_listeners: [TobListener] = []

    def register_trade_listener(self, listener: TradeListener):
        if listener not in self.trade_listeners:
            self.trade_listeners.append(listener)

    def register_tob_listener(self, listener: TobListener):
        if listener not in self.tob_listeners:
            self.tob_listeners.append(listener)

    def notify_trade(self, trade: Trade):
        for trade_listener in self.trade_listeners:
            trade_listener.on_trade(trade)

    def print_tob(self):
        bid_price = f"{self.top_of_book.bid_price / 10000:.2f}" if self.top_of_book.bid_price else ""
        ask_price = f"{self.top_of_book.ask_price / 10000:.2f}" if self.top_of_book.ask_price else ""
        bid_size = f"{self.top_of_book.bid_size}"
        ask_size = f"{self.top_of_book.ask_size}"
        print(f"{nanos_to_ms_str(self.timestamp_ns)} {self.stock} {bid_size:>10} {bid_price:>10} | {ask_price:<10} {ask_size}")

    def print_book(self):
        print(f"{self.stock} OrderBook at {TimeUtil.nanos_to_ms_str(self.timestamp_ns)}")
        for bid_level, ask_level in zip_longest(reversed(self.bids.values()), self.asks.values()):
            bid_orders = f"({len(bid_level.orders)})" if bid_level else ''
            bid_size = f"{bid_level.size}" if bid_level else ''
            bid_price = f"{(bid_level.price / 10000):.2f}" if bid_level else ''

            ask_orders = f"({len(ask_level.orders)})" if ask_level else ''
            ask_price = f"{(ask_level.price / 10000):.2f}" if ask_level else ''
            ask_size = f"{ask_level.size}" if ask_level else ''

            print(f"{bid_orders} {bid_size:>10}  {bid_price:>10} | {ask_price:<10} {ask_size:<10} {ask_orders}")

    def order_added(self, order: Order):
        book = self.bids if order.is_bid() else self.asks
        price_level: PriceLevel = book.setdefault(order.price, PriceLevel(order.price))
        price_level.add_order(order)
        self.timestamp_ns = max(self.timestamp_ns, order.timestamp_ns)

    def order_deleted(self, order: Order, timestamp_ns: int):
        book = self.bids if order.is_bid() else self.asks
        try:
            price_level = book[order.price]
            price_level.delete_order(order.id, timestamp_ns)
            self._cleanup_price_level(book, order.price, price_level)
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)
        except KeyError:
            print(f"PriceLevel: {order.price} not found in {self.stock} book.")

    def order_executed(self, order: Order, exec_qty: int, timestamp_ns: int):
        book = self.bids if order.is_bid() else self.asks
        try:
            price_level = book[order.price]
            price_level.order_executed(order.id, exec_qty, timestamp_ns)
            self._cleanup_price_level(book, order.price, price_level)
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)
        except KeyError:
            print(f"PriceLevel: {order.price} not found in {self.stock} book for execution.")

    def order_cancelled(self, order: Order, cancelled_shares: int, timestamp_ns: int):
        book = self.bids if order.is_bid() else self.asks
        try:
            price_level = book[order.price]
            price_level.order_cancelled(order.id, cancelled_shares, timestamp_ns)
            self._cleanup_price_level(book, order.price, price_level)
            self.timestamp_ns = max(self.timestamp_ns, timestamp_ns)
        except KeyError:
            print(f"PriceLevel: {order.price} not found in {self.stock} book for cancel.")

    def _cleanup_price_level(self, book: SortedDict, price: int, price_level: PriceLevel):
        """Remove price level from book if it's empty."""
        if price_level.is_empty():
            del book[price]

    def is_tob_changed(self):
        best_bid: Optional[PriceLevel] = self.bids.peekitem(-1)[1] if self.bids else None
        best_ask: Optional[PriceLevel] = self.asks.peekitem(0)[1] if self.asks else None

        if not best_bid:
            if self.top_of_book.bid_price:
                return True
        elif best_bid.price != self.top_of_book.bid_price and best_bid.size != self.top_of_book.bid_size:
            return True

        if not best_ask:
            if self.top_of_book.ask_price:
                return True
        elif best_ask.price != self.top_of_book.ask_price and best_ask.size != self.top_of_book.ask_size:
            return True

    def notify_tob_change(self):
        if self.is_tob_changed():
            best_bid: Optional[PriceLevel] = self.bids.peekitem(-1)[1] if self.bids else None
            best_ask: Optional[PriceLevel] = self.asks.peekitem(0)[1] if self.asks else None
            self.top_of_book.timestamp = self.timestamp_ns
            self.top_of_book.bid_price = best_bid.price if best_bid else None
            self.top_of_book.bid_size = best_bid.size if best_bid else 0
            self.top_of_book.ask_price = best_ask.price if best_ask else None
            self.top_of_book.ask_size = best_ask.size if best_ask else 0
            for tob_listener in self.tob_listeners:
                tob_listener.on_tob_change(self.top_of_book)
