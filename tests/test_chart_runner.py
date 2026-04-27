import datetime
import struct

from book.trade import Trade, TRADE_TYPE_ORDER_BOOK as TRADE_TYPE_EXECUTION
from publishers.trade_publisher import _serialize_trade, TRADE_FORMAT
from itch.chart_runner import _deserialize_trade


class TestDeserializeRoundTrip:
    def test_full_trade_round_trip(self):
        trade = Trade(
            timestamp_ns=34_200_000_000_000,
            sec_id="AAPL",
            shares=100,
            price=150.25,
            side="B",
            type=TRADE_TYPE_EXECUTION,
            exch_id="XNAS",
            src="itch",
            exch_match_id="99999",
            trade_date=datetime.date(2024, 1, 15),
        )
        payload = _serialize_trade(trade)
        result = _deserialize_trade(payload)

        assert result["s"] == "AAPL"
        assert abs(result["p"] - 150.25) < 1e-9
        assert result["v"] == 100
        assert result["sd"] == "B"
        # epoch should be trade_date midnight UTC + timestamp_ns in seconds
        assert result["t"] > 0

    def test_empty_side_round_trip(self):
        trade = Trade(
            timestamp_ns=1_000_000_000,
            sec_id="MSFT",
            shares=5000,
            price=250.0,
            side="",
            type="C",
            trade_date=datetime.date(2024, 6, 1),
        )
        payload = _serialize_trade(trade)
        result = _deserialize_trade(payload)

        assert result["s"] == "MSFT"
        assert abs(result["p"] - 250.0) < 1e-9
        assert result["v"] == 5000

    def test_short_stock_padded(self):
        trade = Trade(
            timestamp_ns=500,
            sec_id="X",
            shares=1,
            price=10.0,
            side="S",
            type="N",
            trade_date=datetime.date(2024, 1, 15),
        )
        payload = _serialize_trade(trade)
        result = _deserialize_trade(payload)

        assert result["s"] == "X"  # trailing spaces stripped
        assert result["sd"] == "S"

    def test_payload_size(self):
        trade = Trade(
            timestamp_ns=0, sec_id="TEST", shares=1, price=1.0, side="B", type="E",
            trade_date=datetime.date(2024, 1, 15),
        )
        payload = _serialize_trade(trade)
        assert len(payload) == struct.calcsize(TRADE_FORMAT)
