from datetime import datetime
from collections import defaultdict
from pathlib import Path
from typing import Optional

from book.order_book import OrderBook
from book.top_of_book import TopOfBook
from book.order import Order
from book.trade import Trade, TRADE_TYPE_EXECUTION, TRADE_TYPE_EXECUTION_WITH_PRICE, TRADE_TYPE_NON_CROSS, \
    TRADE_TYPE_OPEN_CROSS, TRADE_TYPE_CLOSE_CROSS, TRADE_TYPE_IPO_OR_HALT, TRADE_TYPE_UNKNOWN
from book.trade_listener import TradeListener
from itch.tob_publisher import TopOfBookPublisher
from itch.itch_listener import ItchListener
from itch.itch_parser import ItchParser


def print_books(*stocks):
    for stock in stocks:
        stock_book = feedHandler.book_map.get(stock)
        if stock_book:
            stock_book.print_book()
            # stock_tob = stock_book.get_top_of_book()
            # print(f"{stock}: {stock_tob}")


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

    def __init__(self, exch_id: str = "", src: str = "", trade_date: Optional[datetime.date] = None):
        self.order_map: dict[int, Order] = {}
        self.book_map: dict[str, OrderBook] = {}
        self.tob_map: dict[str, TopOfBook] = {}
        self.tob_publisher: Optional[TopOfBookPublisher] = None
        self.trade_listener: Optional[TradeListener] = None
        self.timestamp = 0
        # Session metadata
        self.exch_id = exch_id
        self.src = src
        self.trade_date = trade_date
        # MPID tracking
        self.mpid_order_count: int = 0
        self.mpid_counts: dict[str, int] = defaultdict(int)
        # Parser for pipeline
        self.parser: ItchParser = ItchParser()
        self.parser.set_listener(self)

    def process_file(self, itch_file_path: Path) -> None:
        """Process an ITCH file through the parser pipeline."""
        self.parser.parse_file(itch_file_path)

    def register_publisher(self, tob_publisher):
        self.tob_publisher = tob_publisher

    def register_trade_listener(self, listener: TradeListener):
        self.trade_listener = listener
        for book in self.book_map.values():
            book.register_trade_listener(listener)

    def _get_or_create_book(self, stock: str) -> OrderBook:
        book = self.book_map.get(stock)
        if book is None:
            book = OrderBook(stock)
            if self.trade_listener:
                book.register_trade_listener(self.trade_listener)
            self.book_map[stock] = book
        return book

    def handle_order_add(self, order: Order):
        self.timestamp = max(self.timestamp, order.timestamp_ns)
        stock = order.stock
        old_order = self.order_map.get(order.id)
        if old_order is not None:
            print(f"Duplicate orderId:{order.id}! Old: {old_order}, New: {order}")
        self.order_map[order.id] = order
        order_book = self._get_or_create_book(stock)
        order_book.order_added(order)
        if self.tob_publisher:
            self.publish_tob(order_book)

    def handle_order_delete(self, order_id: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order = self.order_map.get(order_id)
        if not order:
            return

        order.timestamp_ns = timestamp_ns
        order_book = self.book_map.get(order.stock)
        if not order_book:
            print(f"order deleted but no orderbook for stock: {order.stock}")
            return

        order_book.order_deleted(order, timestamp_ns)
        if self.tob_publisher:
            self.publish_tob(order_book)

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

        order_book.order_executed(order, exec_qty, timestamp_ns)
        if self.tob_publisher:
            self.publish_tob(order_book)

        trade = Trade(
            timestamp_ns=timestamp_ns,
            sec_id=order.stock,
            shares=exec_qty,
            price=order.price / 10000,
            side=order.buy_sell,
            type=TRADE_TYPE_EXECUTION,
            exch_id=self.exch_id,
            src=self.src,
            exch_match_id=str(match_number),
            trade_date=self.trade_date,
        )
        order_book.notify_trade(trade)

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

        order_book.order_executed(order, exec_qty, timestamp_ns)
        if self.tob_publisher:
            self.publish_tob(order_book)

        if printable != 'N':
            trade = Trade(
                timestamp_ns=timestamp_ns,
                sec_id=order.stock,
                shares=exec_qty,
                price=exec_price / 10000,
                side=order.buy_sell,
                type=TRADE_TYPE_EXECUTION_WITH_PRICE,
                exch_id=self.exch_id,
                src=self.src,
                exch_match_id=str(match_number),
                trade_date=self.trade_date,
            )
            order_book.notify_trade(trade)

    def handle_non_cross_trade(self, order_ref: int, buy_sell: str, shares: int, stock: str,
                               price: int, match_number: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order_book = self._get_or_create_book(stock)
        trade = Trade(
            timestamp_ns=timestamp_ns,
            sec_id=stock,
            shares=shares,
            price=price / 10000,
            side=buy_sell,
            type=TRADE_TYPE_NON_CROSS,
            exch_id=self.exch_id,
            src=self.src,
            exch_match_id=str(match_number),
            trade_date=self.trade_date,
        )
        order_book.notify_trade(trade)

    def handle_cross_trade(self, shares: int, stock: str, cross_price: int,
                           match_number: int, cross_type: str, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        order_book = self._get_or_create_book(stock)
        trade = Trade(
            timestamp_ns=timestamp_ns,
            sec_id=stock,
            shares=shares,
            price=cross_price / 10000,
            side="",
            type=get_trade_type(cross_type),
            exch_id=self.exch_id,
            src=self.src,
            exch_match_id=str(match_number),
            trade_date=self.trade_date,
        )
        order_book.notify_trade(trade)

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
        if self.tob_publisher:
            self.publish_tob(order_book)

    def handle_order_replace(self, original_order_id: int, new_order_id: int, new_shares: int, new_price: int, timestamp_ns: int):
        self.timestamp = max(self.timestamp, timestamp_ns)
        # Order replace = delete old order + add new order (time priority changes)
        original_order = self.order_map.get(original_order_id)
        if not original_order:
            return

        # Delete the original order
        self.handle_order_delete(original_order_id, timestamp_ns)
        del self.order_map[original_order_id]

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

    def publish_tob(self, order_book: OrderBook):
        new_tob = order_book.get_top_of_book()
        tob: TopOfBook = self.tob_map.setdefault(order_book.stock, TopOfBook(order_book.stock))
        # if tob != new_tob:
        # tob.update(new_tob)

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


if __name__ == "__main__":
    biz_date = '10302019'
    date_obj = datetime.strptime(biz_date, '%m%d%Y').date()
    itch_file = './data/' + biz_date + '.NASDAQ_ITCH50'
    print(f"itch_file: {itch_file}")
    itch_file_path = Path(itch_file)
    feedHandler = ItchFeedHandler(trade_date=date_obj)

    # TODO: Write a Trade Publisher that writes to Kafka topic (trade-analytics-{trade date}). MsgFormat [msgType][msgSize][msgBytes]
    # TODO: Write a Trade Publisher that writes to Unkaf (sessionId: trade-analytics-{trade date}. MsgFormat [msgType][msgSize][msgBytes]
    # TradePublisher is a Trade Listener.

    # VWap Publisher is a Trade Listener. can publish vwap. VwapCalculator can publish vwap
    # TopOfBook listener -> TopOfBook publisher. Published every second when top-of-book is changed or when last trade


    batch = 1000
    total_msgs = 1_000_000
    with itch_file_path.open('rb') as data:
        msg_processed = 0
        last_pub_timestamp = 0
        while True:
            if not feedHandler.parser.decode(data, batch):
                print(f"Finished processing {itch_file_path}")
                break

            time_lapsed = feedHandler.timestamp - last_pub_timestamp
            # 60 sec
            if time_lapsed > 60 * 1000_000_000:
                for stock, order_book in feedHandler.book_map.items():
                    if stock in ('AAPL', 'TSLA', 'HSBC'):
                        order_book.print_tob()
                    order_book.notify_tob_change()

            msg_processed += batch
            if msg_processed >= total_msgs:
                break

            if msg_processed % 100_000 == 0:
                print_books('AAPL', 'TSLA', 'HSBC')
