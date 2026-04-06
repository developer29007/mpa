import math
import datetime
from unittest.mock import MagicMock, call

from db.inserter import DbInserter, _nan_to_none


def test_nan_to_none():
    assert _nan_to_none(math.nan) is None
    assert _nan_to_none(1.5) == 1.5
    assert _nan_to_none(0.0) == 0.0
    assert _nan_to_none(42) == 42


def _make_inserter():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    trade_date = datetime.date(2019, 10, 30)
    return DbInserter(conn, trade_date), conn, cursor


def test_insert_trades():
    inserter, conn, cursor = _make_inserter()
    trades = [
        {"timestamp_ns": 1000, "sec_id": "AAPL", "shares": 100, "price": 150.0,
         "side": "B", "trade_type": "E", "exch_id": "XNAS", "src": "itch",
         "exch_match_id": 1, "msg_id": 1},
    ]
    inserter.insert_trades(trades)
    assert cursor.executemany.called
    assert trades[0]["trade_date"] == datetime.date(2019, 10, 30)


def test_insert_vwaps_converts_nan():
    inserter, conn, cursor = _make_inserter()
    vwaps = [
        {"timestamp_ns": 2000, "stock": "MSFT", "interval_ms": 1000,
         "vwap_price": math.nan, "volume_traded": 0.0, "shares_traded": 0,
         "trade_count": 0, "msg_id": 5},
    ]
    inserter.insert_vwaps(vwaps)
    assert cursor.executemany.called
    assert vwaps[0]["vwap_price"] is None


def test_insert_tobs_converts_nan():
    inserter, conn, cursor = _make_inserter()
    tobs = [
        {"timestamp_ns": 3000, "stock": "GOOG", "bid_price": math.nan,
         "bid_size": 0, "ask_price": math.nan, "ask_size": 0,
         "last_trade_price": math.nan, "last_trade_timestamp_ns": 0,
         "last_trade_shares": 0, "last_trade_side": " ", "last_trade_type": " ",
         "last_trade_match_id": 0, "msg_id": 10},
    ]
    inserter.insert_tobs(tobs)
    assert cursor.executemany.called
    assert tobs[0]["bid_price"] is None
    assert tobs[0]["ask_price"] is None
    assert tobs[0]["last_trade_price"] is None


def test_flush_commits_and_empty_buffers_skip():
    inserter, conn, cursor = _make_inserter()
    inserter.flush([], [], [])
    # No executemany calls for empty buffers, but commit still happens
    assert conn.commit.called


def test_flush_with_data():
    inserter, conn, cursor = _make_inserter()
    trades = [{"timestamp_ns": 1, "sec_id": "A", "shares": 1, "price": 1.0,
               "side": "B", "trade_type": "E", "exch_id": "", "src": "",
               "exch_match_id": 0, "msg_id": 1}]
    vwaps = [{"timestamp_ns": 2, "stock": "B", "interval_ms": 250,
              "vwap_price": 1.0, "volume_traded": 1.0, "shares_traded": 1,
              "trade_count": 1, "msg_id": 2}]
    tobs = [{"timestamp_ns": 3, "stock": "C", "bid_price": 1.0, "bid_size": 1,
             "ask_price": 2.0, "ask_size": 1, "last_trade_price": 1.5,
             "last_trade_timestamp_ns": 0, "last_trade_shares": 0,
             "last_trade_side": " ", "last_trade_type": " ",
             "last_trade_match_id": 0, "msg_id": 3}]
    inserter.flush(trades, vwaps, tobs)
    assert cursor.executemany.call_count == 3
    conn.commit.assert_called_once()
