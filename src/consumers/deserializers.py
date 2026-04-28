import struct

from publishers.trade_bucket_publisher import TRADE_BUCKET_FORMAT
from publishers.market_event_publisher import MARKET_EVENT_FORMAT
from publishers.trade_publisher import TRADE_FORMAT
from publishers.vwap_publisher import VWAP_FORMAT
from publishers.tob_publisher import TOB_FORMAT
from publishers.noii_publisher import NOII_FORMAT


def deserialize_market_event(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, event_type, reason,
    ) = struct.unpack(MARKET_EVENT_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": "",
        "src_id": "",
        "event_type": event_type.decode("ascii").strip(),
        "reason": reason.decode("ascii").strip(),
    }


def deserialize_trade(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, shares, price,
        side, trade_type, exch_id, src_id, exch_match_id,
    ) = struct.unpack(TRADE_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": exch_id.decode("ascii").strip(),
        "src_id": src_id.decode("ascii").strip(),
        "shares": shares,
        "price": price,
        "side": side.decode("ascii"),
        "trade_type": trade_type.decode("ascii"),
        "exch_match_id": exch_match_id,
    }


def deserialize_vwap(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, interval_ms,
        vwap_price, volume_traded, shares_traded, trade_count,
    ) = struct.unpack(VWAP_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": "",
        "src_id": "",
        "interval_ms": interval_ms,
        "vwap_price": vwap_price,
        "volume_traded": volume_traded,
        "shares_traded": shares_traded,
        "trade_count": trade_count,
    }


def deserialize_tob(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, bid_price, bid_size,
        ask_price, ask_size, last_trade_price, last_trade_ts_me,
        last_trade_shares, last_trade_side, last_trade_type, last_trade_match_id,
    ) = struct.unpack(TOB_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": "",
        "src_id": "",
        "bid_price": bid_price,
        "bid_size": bid_size,
        "ask_price": ask_price,
        "ask_size": ask_size,
        "last_trade_price": last_trade_price,
        "last_trade_ts_me": last_trade_ts_me,
        "last_trade_shares": last_trade_shares,
        "last_trade_side": last_trade_side.decode("ascii"),
        "last_trade_type": last_trade_type.decode("ascii"),
        "last_trade_match_id": last_trade_match_id,
    }


def deserialize_noii(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, paired_shares, imbalance_shares,
        far_price, near_price, current_reference_price,
        imbalance_direction, cross_type, price_variation_indicator,
    ) = struct.unpack(NOII_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": "",
        "src_id": "",
        "paired_shares": paired_shares,
        "imbalance_shares": imbalance_shares,
        "far_price": far_price,
        "near_price": near_price,
        "current_reference_price": current_reference_price,
        "imbalance_direction": imbalance_direction.decode("ascii"),
        "cross_type": cross_type.decode("ascii"),
        "price_variation_indicator": price_variation_indicator.decode("ascii"),
    }


def deserialize_trade_bucket(payload: bytes) -> dict:
    (
        msg_seq, ts_me, sec_id, interval_ms,
        open_, high, low, close, notional, vwap,
        total_shares, buy_shares, sell_shares, auction_shares, hidden_shares, trade_count,
        buy_volume, sell_volume, auction_volume, hidden_volume,
    ) = struct.unpack(TRADE_BUCKET_FORMAT, payload)
    return {
        "msg_seq": msg_seq,
        "ts_me": ts_me,
        "sec_id": sec_id.decode("ascii").strip(),
        "exch_id": "",
        "src_id": "",
        "interval_ms": interval_ms,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "notional": notional,
        "vwap": vwap,
        "total_shares": total_shares,
        "buy_shares": buy_shares,
        "sell_shares": sell_shares,
        "auction_shares": auction_shares,
        "hidden_shares": hidden_shares,
        "trade_count": trade_count,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "auction_volume": auction_volume,
        "hidden_volume": hidden_volume,
    }
