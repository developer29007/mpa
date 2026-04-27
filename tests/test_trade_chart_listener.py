import calendar
import datetime
import json
from unittest.mock import patch, MagicMock

from book.trade import Trade, TRADE_TYPE_ORDER_BOOK as TRADE_TYPE_EXECUTION
from web.trade_chart_listener import _trade_to_json, TradeChartListener

TEST_DATE = datetime.date(2024, 1, 15)


class TestTradeToJson:
    def test_field_mapping(self):
        trade = Trade(
            timestamp_ns=34_200_000_000_000,  # 09:30:00.000 in ns
            sec_id="AAPL",
            shares=100,
            price=150.25,
            side="B",
            type=TRADE_TYPE_EXECUTION,
            trade_date=TEST_DATE,
        )
        result = json.loads(_trade_to_json(trade))
        assert result["s"] == "AAPL"
        assert result["p"] == 150.25
        assert result["v"] == 100
        assert result["sd"] == "B"

    def test_epoch_time_correctness(self):
        trade = Trade(
            timestamp_ns=1_000_000_000,  # 1 second
            sec_id="TSLA",
            shares=50,
            price=200.0,
            side="S",
            type=TRADE_TYPE_EXECUTION,
            trade_date=TEST_DATE,
        )
        result = json.loads(_trade_to_json(trade))
        expected_epoch = calendar.timegm(TEST_DATE.timetuple()) + 1.0
        assert result["t"] == expected_epoch

    def test_trailing_space_stripping(self):
        trade = Trade(
            timestamp_ns=0,
            sec_id="X       ",  # padded with spaces
            shares=10,
            price=50.0,
            side="B ",
            type=TRADE_TYPE_EXECUTION,
            trade_date=TEST_DATE,
        )
        result = json.loads(_trade_to_json(trade))
        assert result["s"] == "X"
        assert result["sd"] == "B"

    def test_empty_side(self):
        trade = Trade(
            timestamp_ns=0,
            sec_id="MSFT",
            shares=5000,
            price=250.0,
            side="",
            type="C",
            trade_date=TEST_DATE,
        )
        result = json.loads(_trade_to_json(trade))
        assert result["sd"] == ""

    def test_compact_json_no_spaces(self):
        trade = Trade(
            timestamp_ns=0,
            sec_id="AAPL",
            shares=100,
            price=150.0,
            side="B",
            type="E",
            trade_date=TEST_DATE,
        )
        raw = _trade_to_json(trade)
        assert " " not in raw  # compact, no whitespace


class TestTradeChartListenerFiltering:
    @patch("web.trade_chart_listener.ChartServer")
    def test_filters_by_stock_set(self, mock_server_cls):
        mock_server = MagicMock()
        mock_server_cls.return_value = mock_server

        listener = TradeChartListener(stocks={"AAPL", "TSLA"})

        trade_aapl = Trade(timestamp_ns=0, sec_id="AAPL", shares=100, price=150.0, side="B", type="E", trade_date=TEST_DATE)
        trade_goog = Trade(timestamp_ns=0, sec_id="GOOG", shares=50, price=100.0, side="S", type="E", trade_date=TEST_DATE)
        trade_tsla = Trade(timestamp_ns=0, sec_id="TSLA", shares=200, price=200.0, side="B", type="E", trade_date=TEST_DATE)

        listener.on_trade(trade_aapl)
        listener.on_trade(trade_goog)
        listener.on_trade(trade_tsla)

        assert mock_server.enqueue_trade.call_count == 2

    @patch("web.trade_chart_listener.ChartServer")
    def test_none_stocks_accepts_all(self, mock_server_cls):
        mock_server = MagicMock()
        mock_server_cls.return_value = mock_server

        listener = TradeChartListener(stocks=None)

        trade1 = Trade(timestamp_ns=0, sec_id="AAPL", shares=100, price=150.0, side="B", type="E", trade_date=TEST_DATE)
        trade2 = Trade(timestamp_ns=0, sec_id="GOOG", shares=50, price=100.0, side="S", type="E", trade_date=TEST_DATE)

        listener.on_trade(trade1)
        listener.on_trade(trade2)

        assert mock_server.enqueue_trade.call_count == 2
