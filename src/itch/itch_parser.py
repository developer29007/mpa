from pathlib import Path
import struct
from typing import Optional

from book.order import Order
from itch.itch_listener import ItchListener

# Format strings for all message types (excluding message type byte which is already decoded)

# System messages
SYSTEM_EVENT_FMT = '>HH6sc'  # stock_locate, tracking_number, timestamp(6s), event_code
STOCK_DIRECTORY_FMT = '>HH6s8sc8sccIccc4scccc'  # Complex format for stock directory
STOCK_TRADING_ACTION_FMT = '>HH6s8scc4s'  # stock_locate, tracking, timestamp, stock, trading_state, reserved, reason
REG_SHO_FMT = '>HH6s8sc'  # stock_locate, tracking, timestamp, stock, reg_sho_action
MARKET_PARTICIPANT_POSITION_FMT = '>HH6s4s8sccc'  # stock_locate, tracking, timestamp, mpid, stock, primary_mm, mm_mode, mp_state
MWCB_DECLINE_LEVEL_FMT = '>HH6s8s8s8s'  # stock_locate, tracking, timestamp, level1, level2, level3 (prices are 8 bytes)
MWCB_STATUS_FMT = '>HH6sc'  # stock_locate, tracking, timestamp, breached_level
IPO_QUOTING_PERIOD_FMT = '>HH6s8sIcI'  # stock_locate, tracking, timestamp, stock, ipo_quotation_time, ipo_qualifier, ipo_price
LULD_AUCTION_COLLAR_FMT = '>HH6s8sIIII'  # stock_locate, tracking, timestamp, stock, ref_price, upper_price, lower_price, extension
OPERATIONAL_HALT_FMT = '>HH6s8scc'  # stock_locate, tracking, timestamp, stock, market_code, halt_action

ADD_ORDER_FMT = '>HH6sQcI8sI'
# Add Order messages (already have 'A', here's 'F')
ADD_ORDER_MPID_FMT = '>HH6sQcI8sI4s'  # stock_locate, tracking, timestamp(6s), order_ref, buy_sell, shares, stock, price, attribution

# Modify Order messages
ORDER_EXECUTED_FMT = '>HH6sQIQ'  # stock_locate, tracking, timestamp(6s), order_ref, executed_shares, match_number
ORDER_EXECUTED_PRICE_FMT = '>HH6sQIQcI'  # stock_locate, tracking, timestamp(6s), order_ref, executed_shares, match_number, printable, exec_price
ORDER_CANCEL_FMT = '>HH6sQI'  # stock_locate, tracking, timestamp(6s), order_ref, cancelled_shares
ORDER_DELETE_FMT = '>HH6sQ'  # stock_locate, tracking, timestamp(6s), order_ref
ORDER_REPLACE_FMT = '>HH6sQQII'  # stock_locate, tracking, timestamp(6s), original_order_ref, new_order_ref, shares, price

# Trade messages
TRADE_NON_CROSS_FMT = '>HH6sQcI8sIQ'  # stock_locate, tracking, timestamp(6s), order_ref, buy_sell, shares, stock, price, match_number
CROSS_TRADE_FMT = '>HH6sQ8sIQc'  # stock_locate, tracking, timestamp(6s), shares, stock, cross_price, match_number, cross_type
BROKEN_TRADE_FMT = '>HH6sQ'  # stock_locate, tracking, timestamp(6s), match_number

# NOII message
NOII_FMT = '>HH6sQQc8sIIIcc'  # stock_locate, tracking, timestamp(6s), paired_shares, imbalance_shares, imbalance_direction, stock, far_price, near_price, current_ref_price, cross_type, price_var_indicator

# Retail Price Improvement Indicator
RPII_FMT = '>HH6s8sc'  # stock_locate, tracking, timestamp(6s), stock, interest_flag

# Direct Listing
DIRECT_LISTING_FMT = '>HH6s8scIIIQII'  # stock_locate, tracking, timestamp(6s), stock, open_eligibility, min_price, max_price, near_exec_price, near_exec_time, lower_collar, upper_collar



def parse_add_order_message(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell, shares, stock, price_raw) = struct.unpack(
        ADD_ORDER_FMT, bits)

    # Convert 6-byte timestamp to integer
    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        # 'msg_type': msg_type.decode('ascii'),
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'buy_sell': buy_sell.decode('ascii'),
        'shares': shares,
        'stock': stock.decode('ascii').rstrip(),
        'price': price_raw  # Keep as raw int, 4 decimal precision (divide by 10000 for dollars)
    }

def parse_system_event(bits):
    (stock_locate, tracking_number, timestamp_bytes, event_code) = struct.unpack(SYSTEM_EVENT_FMT, bits)
    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'event_code': event_code.decode('ascii')
    }


def parse_stock_directory(bits):
    # Complex message - need to parse carefully
    (stock_locate, tracking_number, timestamp_bytes, stock, market_category, financial_status,
     round_lot_size, round_lots_only, issue_classification, issue_subtype, authenticity,
     short_sale_threshold, ipo_flag, luld_ref_price_tier, etp_flag, etp_leverage_factor,
     inverse_indicator) = struct.unpack(STOCK_DIRECTORY_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'market_category': market_category.decode('ascii'),
        'financial_status': financial_status.decode('ascii'),
        'round_lot_size': round_lot_size,
        'round_lots_only': round_lots_only.decode('ascii'),
        'issue_classification': issue_classification.decode('ascii'),
        'issue_subtype': issue_subtype.decode('ascii'),
        'authenticity': authenticity.decode('ascii'),
        'short_sale_threshold': short_sale_threshold.decode('ascii'),
        'ipo_flag': ipo_flag.decode('ascii'),
        'luld_ref_price_tier': luld_ref_price_tier.decode('ascii'),
        'etp_flag': etp_flag.decode('ascii'),
        'etp_leverage_factor': etp_leverage_factor,
        'inverse_indicator': inverse_indicator.decode('ascii')
    }


def parse_stock_trading_action(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock, trading_state,
     reserved, reason) = struct.unpack(STOCK_TRADING_ACTION_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'trading_state': trading_state.decode('ascii'),
        'reserved': reserved.decode('ascii'),
        'reason': reason.decode('ascii').rstrip()
    }


def parse_reg_sho(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock,
     reg_sho_action) = struct.unpack(REG_SHO_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'reg_sho_action': reg_sho_action.decode('ascii')
    }


def parse_market_participant_position(bits):
    (stock_locate, tracking_number, timestamp_bytes, mpid, stock, primary_mm,
     mm_mode, mp_state) = struct.unpack(MARKET_PARTICIPANT_POSITION_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'mpid': mpid.decode('ascii').rstrip(),
        'stock': stock.decode('ascii').rstrip(),
        'primary_market_maker': primary_mm.decode('ascii'),
        'market_maker_mode': mm_mode.decode('ascii'),
        'market_participant_state': mp_state.decode('ascii')
    }


def parse_mwcb_decline_level(bits):
    (stock_locate, tracking_number, timestamp_bytes, level1, level2,
     level3) = struct.unpack(MWCB_DECLINE_LEVEL_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'level1': int.from_bytes(level1, byteorder='big', signed=False) / 100000000.0,
        'level2': int.from_bytes(level2, byteorder='big', signed=False) / 100000000.0,
        'level3': int.from_bytes(level3, byteorder='big', signed=False) / 100000000.0
    }


def parse_mwcb_status(bits):
    (stock_locate, tracking_number, timestamp_bytes,
     breached_level) = struct.unpack(MWCB_STATUS_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'breached_level': breached_level.decode('ascii')
    }


def parse_ipo_quoting_period(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock, ipo_quotation_time,
     ipo_qualifier, ipo_price) = struct.unpack(IPO_QUOTING_PERIOD_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'ipo_quotation_release_time': ipo_quotation_time,
        'ipo_quotation_release_qualifier': ipo_qualifier.decode('ascii'),
        'ipo_price': ipo_price  # Keep as raw int, 4 decimal precision
    }


def parse_luld_auction_collar(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock, ref_price, upper_price,
     lower_price, extension) = struct.unpack(LULD_AUCTION_COLLAR_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'auction_collar_reference_price': ref_price,  # Keep as raw int, 4 decimal precision
        'upper_auction_collar_price': upper_price,  # Keep as raw int, 4 decimal precision
        'lower_auction_collar_price': lower_price,  # Keep as raw int, 4 decimal precision
        'auction_collar_extension': extension
    }


def parse_operational_halt(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock, market_code,
     halt_action) = struct.unpack(OPERATIONAL_HALT_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'market_code': market_code.decode('ascii'),
        'operational_halt_action': halt_action.decode('ascii')
    }


def parse_add_order_mpid(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell, shares,
     stock, price_raw, attribution) = struct.unpack(ADD_ORDER_MPID_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'buy_sell': buy_sell.decode('ascii'),
        'shares': shares,
        'stock': stock.decode('ascii').rstrip(),
        'price': price_raw,  # Keep as raw int, 4 decimal precision
        'attribution': attribution.decode('ascii').rstrip()
    }


def parse_order_executed(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref, executed_shares,
     match_number) = struct.unpack(ORDER_EXECUTED_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'executed_shares': executed_shares,
        'match_number': match_number
    }


def parse_order_executed_with_price(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref, executed_shares,
     match_number, printable, exec_price) = struct.unpack(ORDER_EXECUTED_PRICE_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'executed_shares': executed_shares,
        'match_number': match_number,
        'printable': printable.decode('ascii'),
        'execution_price': exec_price  # Keep as raw int, 4 decimal precision
    }


def parse_order_cancel(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref,
     cancelled_shares) = struct.unpack(ORDER_CANCEL_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'cancelled_shares': cancelled_shares
    }


def parse_order_delete(bits):
    (stock_locate, tracking_number, timestamp_bytes,
     order_ref) = struct.unpack(ORDER_DELETE_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref
    }


def parse_order_replace(bits):
    (stock_locate, tracking_number, timestamp_bytes, original_order_ref,
     new_order_ref, shares, price) = struct.unpack(ORDER_REPLACE_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'original_order_ref': original_order_ref,
        'new_order_ref': new_order_ref,
        'shares': shares,
        'price': price  # Keep as raw int, 4 decimal precision
    }


def parse_trade_non_cross(bits):
    (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell, shares,
     stock, price_raw, match_number) = struct.unpack(TRADE_NON_CROSS_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'order_ref': order_ref,
        'buy_sell': buy_sell.decode('ascii'),
        'shares': shares,
        'stock': stock.decode('ascii').rstrip(),
        'price': price_raw,  # Keep as raw int, 4 decimal precision
        'match_number': match_number
    }


def parse_cross_trade(bits):
    (stock_locate, tracking_number, timestamp_bytes, shares, stock, cross_price,
     match_number, cross_type) = struct.unpack(CROSS_TRADE_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'shares': shares,
        'stock': stock.decode('ascii').rstrip(),
        'cross_price': cross_price,  # Keep as raw int, 4 decimal precision
        'match_number': match_number,
        'cross_type': cross_type.decode('ascii')
    }


def parse_broken_trade(bits):
    (stock_locate, tracking_number, timestamp_bytes,
     match_number) = struct.unpack(BROKEN_TRADE_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'match_number': match_number
    }


def parse_noii(bits):
    (stock_locate, tracking_number, timestamp_bytes, paired_shares, imbalance_shares,
     imbalance_direction, stock, far_price, near_price, current_ref_price, cross_type,
     price_var_indicator) = struct.unpack(NOII_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'paired_shares': paired_shares,
        'imbalance_shares': imbalance_shares,
        'imbalance_direction': imbalance_direction.decode('ascii'),
        'stock': stock.decode('ascii').rstrip(),
        'far_price': far_price,  # Keep as raw int, 4 decimal precision
        'near_price': near_price,  # Keep as raw int, 4 decimal precision
        'current_reference_price': current_ref_price,  # Keep as raw int, 4 decimal precision
        'cross_type': cross_type.decode('ascii'),
        'price_variation_indicator': price_var_indicator.decode('ascii')
    }


def parse_rpii(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock,
     interest_flag) = struct.unpack(RPII_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'interest_flag': interest_flag.decode('ascii')
    }


def parse_direct_listing(bits):
    (stock_locate, tracking_number, timestamp_bytes, stock, open_eligibility, min_price,
     max_price, near_exec_price, near_exec_time, lower_collar,
     upper_collar) = struct.unpack(DIRECT_LISTING_FMT, bits)

    timestamp = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)

    return {
        'stock_locate': stock_locate,
        'tracking_number': tracking_number,
        'timestamp': timestamp,
        'stock': stock.decode('ascii').rstrip(),
        'open_eligibility_status': open_eligibility.decode('ascii'),
        'minimum_allowable_price': min_price,  # Keep as raw int, 4 decimal precision
        'maximum_allowable_price': max_price,  # Keep as raw int, 4 decimal precision
        'near_execution_price': near_exec_price,  # Keep as raw int, 4 decimal precision
        'near_execution_time': near_exec_time,
        'lower_price_range_collar': lower_collar,  # Keep as raw int, 4 decimal precision
        'upper_price_range_collar': upper_collar  # Keep as raw int, 4 decimal precision
    }


# Message parser dispatch dictionary
MESSAGE_PARSERS = {
    'S': parse_system_event,
    'R': parse_stock_directory,
    'H': parse_stock_trading_action,
    'Y': parse_reg_sho,
    'L': parse_market_participant_position,
    'V': parse_mwcb_decline_level,
    'W': parse_mwcb_status,
    'K': parse_ipo_quoting_period,
    'J': parse_luld_auction_collar,
    'h': parse_operational_halt,
    'A': parse_add_order_message,  # Your existing function
    'F': parse_add_order_mpid,
    'E': parse_order_executed,
    'C': parse_order_executed_with_price,
    'X': parse_order_cancel,
    'D': parse_order_delete,
    'U': parse_order_replace,
    'P': parse_trade_non_cross,
    'Q': parse_cross_trade,
    'B': parse_broken_trade,
    'I': parse_noii,
    'N': parse_rpii,
    'O': parse_direct_listing
}


class ItchParser:
    """
    ITCH protocol parser that decodes binary ITCH data and notifies listeners.
    """

    def __init__(self):
        self.listener: Optional[ItchListener] = None

    def set_listener(self, listener: ItchListener) -> None:
        """Register a listener to receive decoded ITCH messages."""
        self.listener = listener

    def decode(self, data, msgCount: int):
        count = 0
        while count < msgCount:
            msg_bytes = data.read(2)
            if not msg_bytes:
                return False
            msg_size = struct.unpack('>H', msg_bytes)[0]
            msg_type = data.read(1).decode('ascii')
            msg_data = data.read(msg_size - 1)

            self._parse_and_dispatch(msg_type, msg_data)
            count += 1
        return True

    def parse_file(self, itch_file_path: Path) -> None:
        """Parse an ITCH file and dispatch messages to the listener."""
        with itch_file_path.open('rb') as data:
            while True:
                msg_bytes = data.read(2)
                if not msg_bytes:
                    break
                msg_size = struct.unpack('>H', msg_bytes)[0]
                msg_type = data.read(1).decode('ascii')
                msg_data = data.read(msg_size - 1)

                self._parse_and_dispatch(msg_type, msg_data)

    def _parse_and_dispatch(self, msg_type: str, msg_data: bytes) -> None:
        """Parse message and dispatch directly to listener."""
        if not self.listener:
            return

        if msg_type == 'A':  # Add Order
            self._parse_add_order(msg_data)
        elif msg_type == 'F':  # Add Order with MPID
            self._parse_add_order_mpid(msg_data)
        elif msg_type == 'E':  # Order Executed
            self._parse_order_executed(msg_data)
        elif msg_type == 'C':  # Order Executed with Price
            self._parse_order_executed_with_price(msg_data)
        elif msg_type == 'X':  # Order Cancel
            self._parse_order_cancel(msg_data)
        elif msg_type == 'D':  # Order Delete
            self._parse_order_delete(msg_data)
        elif msg_type == 'U':  # Order Replace
            self._parse_order_replace(msg_data)
        elif msg_type == 'P':  # Non-Cross Trade
            self._parse_trade_non_cross(msg_data)
        elif msg_type == 'Q':  # Cross Trade
            self._parse_cross_trade(msg_data)

    def _parse_add_order(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell,
         shares, stock, price) = struct.unpack(ADD_ORDER_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        order = Order(
            id=order_ref,
            timestamp_ns=timestamp_ns,
            stock=stock.decode('ascii').rstrip(),
            buy_sell=buy_sell.decode('ascii'),
            shares=shares,
            price=price
        )
        self.listener.on_add_order(order)

    def _parse_add_order_mpid(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell,
         shares, stock, price, attribution) = struct.unpack(ADD_ORDER_MPID_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        order = Order(
            id=order_ref,
            timestamp_ns=timestamp_ns,
            stock=stock.decode('ascii').rstrip(),
            buy_sell=buy_sell.decode('ascii'),
            shares=shares,
            price=price
        )
        self.listener.on_add_order_mpid(order, attribution.decode('ascii').rstrip())

    def _parse_order_executed(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref,
         executed_shares, match_number) = struct.unpack(ORDER_EXECUTED_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_order_executed(order_ref, executed_shares, match_number, timestamp_ns)

    def _parse_order_executed_with_price(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref, executed_shares,
         match_number, printable, exec_price) = struct.unpack(ORDER_EXECUTED_PRICE_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_order_executed_with_price(order_ref, executed_shares, exec_price, match_number,
                                                     printable.decode('ascii'), timestamp_ns)

    def _parse_order_cancel(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref,
         cancelled_shares) = struct.unpack(ORDER_CANCEL_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_order_cancel(order_ref, cancelled_shares, timestamp_ns)

    def _parse_order_delete(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes,
         order_ref) = struct.unpack(ORDER_DELETE_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_order_delete(order_ref, timestamp_ns)

    def _parse_order_replace(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, original_order_ref,
         new_order_ref, shares, price) = struct.unpack(ORDER_REPLACE_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_order_replace(original_order_ref, new_order_ref, shares, price, timestamp_ns)

    def _parse_trade_non_cross(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, order_ref, buy_sell, shares,
         stock, price_raw, match_number) = struct.unpack(TRADE_NON_CROSS_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_trade(order_ref, buy_sell.decode('ascii'), shares,
                               stock.decode('ascii').rstrip(), price_raw, match_number, timestamp_ns)

    def _parse_cross_trade(self, msg_data: bytes) -> None:
        (stock_locate, tracking_number, timestamp_bytes, shares, stock, cross_price,
         match_number, cross_type) = struct.unpack(CROSS_TRADE_FMT, msg_data)
        timestamp_ns = int.from_bytes(timestamp_bytes, byteorder='big', signed=False)
        self.listener.on_cross_trade(shares, stock.decode('ascii').rstrip(), cross_price,
                                     match_number, cross_type.decode('ascii'), timestamp_ns)


def decode_itch_file(itch_file_path):
    """Legacy function for backwards compatibility."""
    with itch_file_path.open('rb') as data:
        while True:
            msg_bytes = data.read(2)
            if not msg_bytes:
                break
            msg_size = struct.unpack('>H', msg_bytes)[0]
            msg_type = data.read(1).decode('ascii')
            msg_data = data.read(msg_size - 1)

            if msg_type in MESSAGE_PARSERS:
                parsed = MESSAGE_PARSERS[msg_type](msg_data)
                print(f"{msg_type}: {parsed}")


if __name__ == '__main__':
    date = '10302019'
    itch_file = './data/' + date + '.NASDAQ_ITCH50'
    decode_itch_file(Path(itch_file))
