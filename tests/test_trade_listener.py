import datetime
from typing import List

from book.order import Order
from book.trade import Trade, TRADE_TYPE_EXECUTION, TRADE_TYPE_EXECUTION_WITH_PRICE, TRADE_TYPE_NON_CROSS, TRADE_TYPE_OPEN_CROSS
from book.trade_listener import TradeListener
from itch.itch_feed_handler import ItchFeedHandler


class MockTradeListener(TradeListener):
    def __init__(self):
        self.trades: List[Trade] = []

    def on_trade(self, trade: Trade):
        self.trades.append(trade)


def _add_order(fh: ItchFeedHandler, order_id: int, stock: str, side: str, shares: int, price: int, ts: int = 1000):
    order = Order(id=order_id, timestamp_ns=ts, stock=stock, buy_sell=side, shares=shares, price=price)
    fh.handle_order_add(order)
    return order


class TestOrderExecutedTrade:
    def test_execution_fires_trade_with_order_price(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "B", 100, 1500000)
        fh.handle_order_executed(1, 50, 99999, 2000)

        assert len(listener.trades) == 1
        trade = listener.trades[0]
        assert trade.sec_id == "AAPL"
        assert trade.shares == 50
        assert trade.price == 150.0
        assert trade.side == "B"
        assert trade.type == TRADE_TYPE_EXECUTION
        assert trade.exch_match_id == "99999"
        assert trade.timestamp_ns == 2000

    def test_execution_reduces_book_quantity(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "B", 100, 1500000)
        fh.handle_order_executed(1, 60, 99999, 2000)

        book = fh.book_map["AAPL"]
        price_level = book.bids[1500000]
        assert price_level.size == 40


class TestOrderExecutedWithPricePrintable:
    def test_printable_y_fires_trade_with_exec_price(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "S", 100, 1500000)
        fh.handle_order_executed_with_price(1, 50, 1510000, 88888, "Y", 3000)

        assert len(listener.trades) == 1
        trade = listener.trades[0]
        assert trade.sec_id == "AAPL"
        assert trade.shares == 50
        assert trade.price == 151.0
        assert trade.side == "S"
        assert trade.type == TRADE_TYPE_EXECUTION_WITH_PRICE
        assert trade.exch_match_id == "88888"

    def test_printable_n_no_trade_fired(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "B", 100, 1500000)
        fh.handle_order_executed_with_price(1, 50, 1510000, 88888, "N", 3000)

        assert len(listener.trades) == 0

    def test_printable_n_still_updates_book(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "B", 100, 1500000)
        fh.handle_order_executed_with_price(1, 50, 1510000, 88888, "N", 3000)

        book = fh.book_map["AAPL"]
        price_level = book.bids[1500000]
        assert price_level.size == 50


class TestNonCrossTrade:
    def test_p_message_fires_trade(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        fh.handle_non_cross_trade(12345, "B", 200, "TSLA", 7500000, 77777, 5000)

        assert len(listener.trades) == 1
        trade = listener.trades[0]
        assert trade.sec_id == "TSLA"
        assert trade.shares == 200
        assert trade.price == 750.0
        assert trade.side == "B"
        assert trade.type == TRADE_TYPE_NON_CROSS
        assert trade.exch_match_id == "77777"

    def test_p_message_no_book_modification(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "TSLA", "B", 100, 7500000)
        fh.handle_non_cross_trade(99999, "B", 200, "TSLA", 7500000, 77777, 5000)

        book = fh.book_map["TSLA"]
        price_level = book.bids[7500000]
        assert price_level.size == 100  # unchanged


class TestCrossTrade:
    def test_q_message_fires_trade(self):
        listener = MockTradeListener()
        fh = ItchFeedHandler()
        fh.register_trade_listener(listener)

        fh.handle_cross_trade(5000, "MSFT", 2500000, 66666, "O", 6000)

        assert len(listener.trades) == 1
        trade = listener.trades[0]
        assert trade.sec_id == "MSFT"
        assert trade.shares == 5000
        assert trade.price == 250.0
        assert trade.side == ""
        assert trade.type == TRADE_TYPE_OPEN_CROSS
        assert trade.exch_match_id == "66666"


class TestMetadataPropagation:
    def test_session_metadata_appears_in_trade(self):
        listener = MockTradeListener()
        td = datetime.date(2024, 1, 15)
        fh = ItchFeedHandler(exch_id="XNAS", src="itch_historical", trade_date=td)
        fh.register_trade_listener(listener)

        _add_order(fh, 1, "AAPL", "B", 100, 1500000)
        fh.handle_order_executed(1, 50, 99999, 2000)

        trade = listener.trades[0]
        assert trade.exch_id == "XNAS"
        assert trade.src == "itch_historical"
        assert trade.trade_date == td

    def test_metadata_on_non_cross_trade(self):
        listener = MockTradeListener()
        td = datetime.date(2024, 6, 1)
        fh = ItchFeedHandler(exch_id="XNAS", src="live", trade_date=td)
        fh.register_trade_listener(listener)

        fh.handle_non_cross_trade(1, "S", 100, "GOOG", 1000000, 11111, 7000)

        trade = listener.trades[0]
        assert trade.exch_id == "XNAS"
        assert trade.src == "live"
        assert trade.trade_date == td

    def test_metadata_on_cross_trade(self):
        listener = MockTradeListener()
        td = datetime.date(2024, 6, 1)
        fh = ItchFeedHandler(exch_id="XNAS", src="live", trade_date=td)
        fh.register_trade_listener(listener)

        fh.handle_cross_trade(500, "GOOG", 1000000, 22222, "C", 8000)

        trade = listener.trades[0]
        assert trade.exch_id == "XNAS"
        assert trade.src == "live"
        assert trade.trade_date == td
