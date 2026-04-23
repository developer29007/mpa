from abc import ABC, abstractmethod

from book.order import Order


class ItchListener(ABC):
    """
    Interface for receiving decoded ITCH messages from the parser.
    Implement this interface to handle ITCH feed data.
    """

    @abstractmethod
    def on_add_order(self, order: Order) -> None:
        """Called when an Add Order message (type 'A') is received."""
        pass

    @abstractmethod
    def on_add_order_mpid(self, order: Order, mpid: str) -> None:
        """Called when an Add Order with MPID message (type 'F') is received."""
        pass

    @abstractmethod
    def on_order_executed(self, order_ref: int, executed_shares: int, match_number: int, timestamp_ns: int) -> None:
        """Called when an Order Executed message (type 'E') is received."""
        pass

    @abstractmethod
    def on_order_executed_with_price(self, order_ref: int, executed_shares: int, execution_price: int,
                                     match_number: int, printable: str, timestamp_ns: int) -> None:
        """Called when an Order Executed with Price message (type 'C') is received."""
        pass

    @abstractmethod
    def on_order_cancel(self, order_ref: int, cancelled_shares: int, timestamp_ns: int) -> None:
        """Called when an Order Cancel message (type 'X') is received."""
        pass

    @abstractmethod
    def on_order_delete(self, order_ref: int, timestamp_ns: int) -> None:
        """Called when an Order Delete message (type 'D') is received."""
        pass

    @abstractmethod
    def on_order_replace(self, original_order_ref: int, new_order_ref: int, shares: int, price: int, timestamp_ns: int) -> None:
        """Called when an Order Replace message (type 'U') is received."""
        pass

    @abstractmethod
    def on_trade(self, order_ref: int, buy_sell: str, shares: int, stock: str, price: int,
                 match_number: int, timestamp_ns: int) -> None:
        """Called when a Non-Cross Trade message (type 'P') is received."""
        pass

    @abstractmethod
    def on_cross_trade(self, shares: int, stock: str, cross_price: int, match_number: int,
                       cross_type: str, timestamp_ns: int) -> None:
        """Called when a Cross Trade message (type 'Q') is received."""
        pass

    @abstractmethod
    def on_noii(self, paired_shares: int, imbalance_shares: int, imbalance_direction: str,
                stock: str, far_price: int, near_price: int, current_reference_price: int,
                cross_type: str, price_variation_indicator: str, timestamp_ns: int) -> None:
        """Called when a Net Order Imbalance Indicator message (type 'I') is received."""
        pass

    @abstractmethod
    def on_stock_trading_action(self, stock: str, trading_state: str, reason: str,
                                timestamp_ns: int) -> None:
        """Called when a Stock Trading Action message (type 'H') is received.

        trading_state: 'H'=Halt, 'P'=Pause (LULD), 'Q'=Quote-only, 'T'=Trading
        reason: 4-char reason code (e.g. 'LUDP', 'MWCB', 'SEC ')
        """
        pass
