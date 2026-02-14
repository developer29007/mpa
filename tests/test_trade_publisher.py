import datetime
import struct

from book.trade import Trade, TRADE_TYPE_EXECUTION, TRADE_TYPE_CLOSE_CROSS
from publishers.trade_publisher import _serialize_trade, TRADE_FORMAT


def _unpack_trade(data: bytes):
    return struct.unpack(TRADE_FORMAT, data)


class TestTradeSerializationRoundTrip:
    def test_full_trade_round_trip(self):
        td = datetime.date(2024, 1, 15)
        trade = Trade(
            timestamp_ns=1_000_000_000,
            sec_id="AAPL",
            shares=100,
            price=150.25,
            side="B",
            type=TRADE_TYPE_EXECUTION,
            exch_id="XNAS",
            src="itch",
            exch_match_id="99999",
            trade_date=td,
        )
        data = _serialize_trade(trade)
        ts, sec_id, shares, price, side, ttype, exch_id, src, match_id, td_days = _unpack_trade(data)

        assert ts == 1_000_000_000
        assert sec_id == b'AAPL    '
        assert shares == 100
        assert abs(price - 150.25) < 1e-9
        assert side == b'B'
        assert ttype == b'E'
        assert exch_id == b'XNAS'
        assert src == b'itch    '
        assert match_id == 99999
        assert td_days == 20240115

    def test_empty_optionals(self):
        trade = Trade(
            timestamp_ns=500,
            sec_id="X",
            shares=1,
            price=10.0,
            side="S",
            type="N",
            trade_date=datetime.date(2024, 1, 15),
        )
        data = _serialize_trade(trade)
        ts, sec_id, shares, price, side, ttype, exch_id, src, match_id, td_days = _unpack_trade(data)

        assert ts == 500
        assert sec_id == b'X       '
        assert shares == 1
        assert abs(price - 10.0) < 1e-9
        assert side == b'S'
        assert ttype == b'N'
        assert exch_id == b'    '
        assert src == b'        '
        assert match_id == 0
        assert td_days == 20240115

    def test_cross_trade_empty_side(self):
        trade = Trade(
            timestamp_ns=1000,
            sec_id="MSFT",
            shares=5000,
            price=250.0,
            side="",
            type=TRADE_TYPE_CLOSE_CROSS,
            exch_id="XNAS",
            src="itch",
            exch_match_id="66666",
            trade_date=datetime.date(2024, 6, 1),
        )
        data = _serialize_trade(trade)
        _, _, _, _, side, _, _, _, _, _ = _unpack_trade(data)
        assert side == b' '


class TestTradeSerializationSize:
    def test_payload_is_54_bytes(self):
        trade = Trade(
            timestamp_ns=0, sec_id="TEST", shares=1, price=1.0, side="B", type="E",
            trade_date=datetime.date(2024, 1, 15),
        )
        data = _serialize_trade(trade)
        assert len(data) == struct.calcsize(TRADE_FORMAT)
