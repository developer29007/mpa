import struct

from publishers.trade_publisher import TRADE_FORMAT
from publishers.vwap_publisher import VWAP_FORMAT
from publishers.tob_publisher import TOB_FORMAT


def deserialize_trade(payload: bytes) -> dict:
    """Deserialize binary trade payload."""
    (
        msg_id, timestamp_ns, sec_id, shares, price,
        side, trade_type, exch_id, src, exch_match_id,
    ) = struct.unpack(TRADE_FORMAT, payload)
    return {
        "msg_id": msg_id,
        "timestamp_ns": timestamp_ns,
        "sec_id": sec_id.decode("ascii").strip(),
        "shares": shares,
        "price": price,
        "side": side.decode("ascii"),
        "trade_type": trade_type.decode("ascii"),
        "exch_id": exch_id.decode("ascii").strip(),
        "src": src.decode("ascii").strip(),
        "exch_match_id": exch_match_id,
    }


def deserialize_vwap(payload: bytes) -> dict:
    """Deserialize binary VWAP payload."""
    (
        msg_id, timestamp_ns, stock, interval_ms,
        vwap_price, volume_traded, shares_traded, trade_count,
    ) = struct.unpack(VWAP_FORMAT, payload)
    return {
        "msg_id": msg_id,
        "timestamp_ns": timestamp_ns,
        "stock": stock.decode("ascii").strip(),
        "interval_ms": interval_ms,
        "vwap_price": vwap_price,
        "volume_traded": volume_traded,
        "shares_traded": shares_traded,
        "trade_count": trade_count,
    }


def deserialize_tob(payload: bytes) -> dict:
    """Deserialize binary TOB payload."""
    (
        msg_id, timestamp_ns, stock, bid_price, bid_size,
        ask_price, ask_size, last_trade_price, last_trade_timestamp_ns,
        last_trade_shares, last_trade_side, last_trade_type, last_trade_match_id,
    ) = struct.unpack(TOB_FORMAT, payload)
    return {
        "msg_id": msg_id,
        "timestamp_ns": timestamp_ns,
        "stock": stock.decode("ascii").strip(),
        "bid_price": bid_price,
        "bid_size": bid_size,
        "ask_price": ask_price,
        "ask_size": ask_size,
        "last_trade_price": last_trade_price,
        "last_trade_timestamp_ns": last_trade_timestamp_ns,
        "last_trade_shares": last_trade_shares,
        "last_trade_side": last_trade_side.decode("ascii"),
        "last_trade_type": last_trade_type.decode("ascii"),
        "last_trade_match_id": last_trade_match_id,
    }
