import math
import struct

from consumers.deserializers import deserialize_trade, deserialize_vwap, deserialize_tob
from publishers.trade_publisher import TRADE_FORMAT
from publishers.vwap_publisher import VWAP_FORMAT
from publishers.tob_publisher import TOB_FORMAT


def test_deserialize_trade():
    payload = struct.pack(
        TRADE_FORMAT,
        42,                                  # msg_id
        36_000_000_000_000,                  # timestamp_ns (10:00:00)
        b"AAPL    ",                         # sec_id
        100,                                 # shares
        150.25,                              # price
        b"B",                                # side
        b"E",                                # trade_type
        b"XNAS",                             # exch_id
        b"itch    ",                         # src
        99999,                               # exch_match_id
    )
    result = deserialize_trade(payload)
    assert result["msg_id"] == 42
    assert result["timestamp_ns"] == 36_000_000_000_000
    assert result["sec_id"] == "AAPL"
    assert result["shares"] == 100
    assert result["price"] == 150.25
    assert result["side"] == "B"
    assert result["trade_type"] == "E"
    assert result["exch_id"] == "XNAS"
    assert result["src"] == "itch"
    assert result["exch_match_id"] == 99999


def test_deserialize_vwap():
    payload = struct.pack(
        VWAP_FORMAT,
        7,                                   # msg_id
        36_000_000_000_000,                  # timestamp_ns
        b"MSFT    ",                         # stock
        1000,                                # interval_ms
        285.50,                              # vwap_price
        28550000.0,                          # volume_traded
        100000,                              # shares_traded
        42,                                  # trade_count
    )
    result = deserialize_vwap(payload)
    assert result["msg_id"] == 7
    assert result["timestamp_ns"] == 36_000_000_000_000
    assert result["stock"] == "MSFT"
    assert result["interval_ms"] == 1000
    assert result["vwap_price"] == 285.50
    assert result["volume_traded"] == 28550000.0
    assert result["shares_traded"] == 100000
    assert result["trade_count"] == 42


def test_deserialize_vwap_nan_price():
    payload = struct.pack(
        VWAP_FORMAT,
        8, 36_000_000_000_000, b"GOOG    ", 250,
        math.nan, 0.0, 0, 0,
    )
    result = deserialize_vwap(payload)
    assert math.isnan(result["vwap_price"])
    assert result["shares_traded"] == 0


def test_deserialize_tob():
    payload = struct.pack(
        TOB_FORMAT,
        10,                                  # msg_id
        36_000_000_000_000,                  # timestamp_ns
        b"TSLA    ",                         # stock
        420.50,                              # bid_price
        200,                                 # bid_size
        421.00,                              # ask_price
        150,                                 # ask_size
        420.75,                              # last_trade_price
        35_999_500_000_000,                  # last_trade_timestamp_ns
        50,                                  # last_trade_shares
        b"B",                                # last_trade_side
        b"E",                                # last_trade_type
        88888,                               # last_trade_match_id
    )
    result = deserialize_tob(payload)
    assert result["msg_id"] == 10
    assert result["timestamp_ns"] == 36_000_000_000_000
    assert result["stock"] == "TSLA"
    assert result["bid_price"] == 420.50
    assert result["bid_size"] == 200
    assert result["ask_price"] == 421.00
    assert result["ask_size"] == 150
    assert result["last_trade_price"] == 420.75
    assert result["last_trade_timestamp_ns"] == 35_999_500_000_000
    assert result["last_trade_shares"] == 50
    assert result["last_trade_side"] == "B"
    assert result["last_trade_type"] == "E"
    assert result["last_trade_match_id"] == 88888


def test_deserialize_tob_nan_prices():
    payload = struct.pack(
        TOB_FORMAT,
        11, 36_000_000_000_000, b"NVDA    ",
        math.nan, 0, math.nan, 0,
        math.nan, 0, 0, b" ", b" ", 0,
    )
    result = deserialize_tob(payload)
    assert math.isnan(result["bid_price"])
    assert math.isnan(result["ask_price"])
    assert math.isnan(result["last_trade_price"])
