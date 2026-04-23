import datetime
from collections import defaultdict
from pathlib import Path
from typing import Optional

from book.market_event import (
    MarketEvent,
    EVENT_OPEN_CROSS, EVENT_CLOSE_CROSS, EVENT_IPO_CROSS, EVENT_INTRADAY_CROSS,
    EVENT_HALT, EVENT_PAUSE, EVENT_QUOTATION, EVENT_RESUME,
)
from book.market_event_listener import MarketEventListener
from book.order_book import OrderBook
from book.order import Order
from book.tob_listener import TobListener
from book.trade import TRADE_TYPE_OPEN_CROSS, TRADE_TYPE_CLOSE_CROSS, TRADE_TYPE_IPO_OR_HALT, TRADE_TYPE_UNKNOWN
from book.trade_listener import TradeListener
from itch.itch_listener import ItchListener
from itch.itch_parser import ItchParser

_CROSS_TYPE_TO_EVENT: dict[str, str] = {
    'O': EVENT_OPEN_CROSS,
    'C': EVENT_CLOSE_CROSS,
    'H': EVENT_IPO_CROSS,
    'I': EVENT_INTRADAY_CROSS,
}

_TRADING_STATE_TO_EVENT: dict[str, str] = {
    'H': EVENT_HALT,
    'P': EVENT_PAUSE,
    'Q': EVENT_QUOTATION,
    'T': EVENT_RESUME,
}


def get_trade_type(cross_type: str):
    if cross_type == 'O':
        return TRADE_TYPE_OPEN_CROSS
    elif cross_type == 'C':
        return TRADE_TYPE_CLOSE_CROSS
    elif cross_type == 'H':
        return TRADE_TYPE_IPO_OR_HALT
    else:
        return TRADE_TYPE_UNKNOWN


class ItchFeedHandler(ItchListener):

    def __init__(self, trade_date: datetime.date, exch_id: str = "", src: str = "",
                 stock_filter: set[str] | None = None):
        self.order_map: dict[int, Order] = {}
        self.book_map: dict[str, OrderBook] = {}
        self.trade_listeners: list[TradeListener] = []
        self.tob_listeners: list[TobListener] = []
        self.market_event_listeners: list[MarketEventListener] = []
        self.timestamp = 0
        # Session metadata
        self.exch_id = exch_id
        self.src = src
        self.trade_date = trade_date
        self.stock_filter = stock_filter
        # MPID tracking
        self.mpid_order_count: int = 0
        self.mpid_counts: dict[str, int] = defaultdict(int)
        # Parser for pipeline
        self.parser: ItchParser = ItchParser()
        self.parser.set_listener(self)

    def register_tob_listener(self, listener: TobListener):
        self.tob_listeners.append(listener)
        for book in self.book_map.values():
            book.register_tob_listener(listener)

    def register_market_event_listener(self, listener: MarketEventListener):
        self.market_event_listeners.append(listener)

    def process_file(self, itch_file_path: Path) -> None:
        """Process an ITCH file through the parser pipeline."""
        self.parser.parse_file(itch_file_path)

    def register_trade_listener(self, listener: TradeListener):
        self.trade_listeners.append(listener)
        for book in self.book_map.values():
            book.register_trade_listener(listener)

    def _stock_allowed(self, stock: str) -> bool:
        return self.stock_filter is None or stock in self.stock_filter

    def _get_or_create_book(self, stock: str) -> Optional[OrderBook]:
        if not self._stock_allowed(stock):
            return None
        book = self.book_map.get(stock)
        if book is None:
            book = OrderBook(stock, trade_date=self.trade_date, exch_id=self.exch_id, src=self.src)
            for listener in self.trade_listeners:
                book.register_trade_listener(listener)
            for listener in self.tob_listeners:
                book.register_tob_listener(listener)
            self.book_map[stock] = book
        return book

    def _notify_market_event(self, event: MarketEvent) -> None:
        for listener in self.market_event_listeners:
            listener.on_market_event(event)

    def handle_order_add(self, order: Order):
        self.timestamp = max(self.timestamp, order.timestamp_ns)
        if not self._stock_allowed(order.stock):
            return
        stock = order.stock
        old_order = self.order_map.get(order.id)
        if old_order is not None:
            print(f"Duplicate orderId:{order.id}! Old: {old_order}, New: {order}")
        self.order_map[order.id] = order
        order_book = self._get_or_create_book(stock)
        if order_book:
            order_book.order_added(order)

    def handle_order_delete(self, order_id: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order = self.order_map.get(order_id)
        if not order:
            return

        order.timestamp_ns = timestamp_ns
        del self.order_map[order_id]
        order_book = self.book_map.get(order.stock)
        if not order_book:
            return

        order_book.order_deleted(order, timestamp_ns)

    def handle_order_executed(self, order_id: int, exec_qty: int, match_number: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order = self.order_map.get(order_id)
        if not order:
            return

        order.timestamp_ns = timestamp_ns
        order_book = self.book_map.get(order.stock)
        if not order_book:
            print(f"order executed but no order book for stock: {order.stock}")
            return

        order_book.order_executed(order, exec_qty, match_number, timestamp_ns)

    def handle_order_executed_with_price(self, order_id: int, exec_qty: int, exec_price: int,
                                         match_number: int, printable: str, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order = self.order_map.get(order_id)
        if not order:
            return

        order.timestamp_ns = timestamp_ns
        order_book = self.book_map.get(order.stock)
        if not order_book:
            print(f"order executed but no order book for stock: {order.stock}")
            return

        order_book.order_executed_with_price(order, exec_qty, exec_price, match_number,
                                              printable=(printable != 'N'), timestamp_ns=timestamp_ns)

    def handle_non_cross_trade(self, order_ref: int, buy_sell: str, shares: int, stock: str,
                               price: int, match_number: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order_book = self._get_or_create_book(stock)
        if order_book:
            order_book.record_non_cross_trade(buy_sell, shares, price, match_number, timestamp_ns)

    def handle_cross_trade(self, shares: int, stock: str, cross_price: int,
                           match_number: int, cross_type: str, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order_book = self._get_or_create_book(stock)
        if order_book:
            order_book.record_cross_trade(shares, cross_price, match_number, get_trade_type(cross_type), timestamp_ns)
        event_type = _CROSS_TYPE_TO_EVENT.get(cross_type)
        if event_type and self._stock_allowed(stock):
            self._notify_market_event(MarketEvent(
                timestamp_ns=timestamp_ns,
                stock=stock,
                event_type=event_type,
                trade_date=self.trade_date,
                price=cross_price / 10000.0,
                shares=shares,
            ))

    def handle_order_cancel(self, order_id: int, cancelled_shares: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order = self.order_map.get(order_id)
        if not order:
            return

        order.timestamp_ns = timestamp_ns
        order_book = self.book_map.get(order.stock)
        if not order_book:
            print(f"order cancelled but no order book for stock: {order.stock}")
            return

        order_book.order_cancelled(order, cancelled_shares, timestamp_ns)

    def handle_order_replace(self, original_order_id: int, new_order_id: int, new_shares: int, new_price: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        # Order replace = delete old order + add new order (time priority changes)
        original_order = self.order_map.get(original_order_id)
        if not original_order:
            return

        # Delete the original order
        self.handle_order_delete(original_order_id, timestamp_ns)

        # Create and add the new order with same side and stock
        new_order = Order(
            id=new_order_id,
            timestamp_ns=timestamp_ns,
            stock=original_order.stock,
            buy_sell=original_order.buy_sell,
            shares=new_shares,
            price=new_price
        )
        self.handle_order_add(new_order)

    def handle_add_order_mpid(self, order: Order, mpid: str):
        # Track MPID statistics
        self.mpid_order_count += 1
        self.mpid_counts[mpid] += 1
        # Delegate to regular add_order
        self.handle_order_add(order)

    # ItchListener interface implementation
    def on_add_order(self, order: Order) -> None:
        self.handle_order_add(order)

    def on_add_order_mpid(self, order: Order, mpid: str) -> None:
        self.handle_add_order_mpid(order, mpid)

    def on_order_executed(self, order_ref: int, executed_shares: int, match_number: int, timestamp_ns: int) -> None:
        self.handle_order_executed(order_ref, executed_shares, match_number, timestamp_ns)

    def on_order_executed_with_price(self, order_ref: int, executed_shares: int, execution_price: int,
                                     match_number: int, printable: str, timestamp_ns: int) -> None:
        self.handle_order_executed_with_price(order_ref, executed_shares, execution_price,
                                              match_number, printable, timestamp_ns)

    def on_order_cancel(self, order_ref: int, cancelled_shares: int, timestamp_ns: int) -> None:
        self.handle_order_cancel(order_ref, cancelled_shares, timestamp_ns)

    def on_order_delete(self, order_ref: int, timestamp_ns: int) -> None:
        self.handle_order_delete(order_ref, timestamp_ns)

    def on_order_replace(self, original_order_ref: int, new_order_ref: int, shares: int, price: int,
                         timestamp_ns: int) -> None:
        self.handle_order_replace(original_order_ref, new_order_ref, shares, price, timestamp_ns)

    def on_trade(self, order_ref: int, buy_sell: str, shares: int, stock: str, price: int,
                 match_number: int, timestamp_ns: int) -> None:
        self.handle_non_cross_trade(order_ref, buy_sell, shares, stock, price, match_number, timestamp_ns)

    def on_cross_trade(self, shares: int, stock: str, cross_price: int, match_number: int,
                       cross_type: str, timestamp_ns: int) -> None:
        self.handle_cross_trade(shares, stock, cross_price, match_number, cross_type, timestamp_ns)

    def on_stock_trading_action(self, stock: str, trading_state: str, reason: str,
                                timestamp_ns: int) -> None:
        self.timestamp = max(self.timestamp, timestamp_ns)
        if not self._stock_allowed(stock):
            return
        event_type = _TRADING_STATE_TO_EVENT.get(trading_state)
        if event_type:
            self._notify_market_event(MarketEvent(
                timestamp_ns=timestamp_ns,
                stock=stock,
                event_type=event_type,
                trade_date=self.trade_date,
                reason=reason,
            ))
