import datetime
from dataclasses import dataclass

TRADE_TYPE_ORDER_BOOK = "E"
TRADE_TYPE_ORDER_BOOK_PRICE = "P"
TRADE_TYPE_NOT_ON_ORDER_BOOK = "N"
TRADE_TYPE_OPEN_CROSS = "O"
TRADE_TYPE_CLOSE_CROSS = "C"
TRADE_TYPE_IPO_OR_HALT = "H"
TRADE_TYPE_UNKNOWN = "U"

_SIDE_LABELS = {"B": "BUY", "S": "SELL"}

_TYPE_LABELS = {
    TRADE_TYPE_ORDER_BOOK:        "'Order Book Execution'",
    TRADE_TYPE_ORDER_BOOK_PRICE:  "'Order Book Execution (Price Changed)'",
    TRADE_TYPE_NOT_ON_ORDER_BOOK: "'Not Displayed on Order Book'",
    TRADE_TYPE_OPEN_CROSS:        "'Opening Cross'",
    TRADE_TYPE_CLOSE_CROSS:       "'Closing Cross'",
    TRADE_TYPE_IPO_OR_HALT:       "'IPO / Halt Resume Cross'",
    TRADE_TYPE_UNKNOWN:           "'Unknown Trade Type'",
}


@dataclass
class Trade:
    # Required trade data
    timestamp_ns: int
    sec_id: str
    shares: int
    price: float
    side: str
    type: str
    trade_date: datetime.date
    # Optional metadata (defaults for lightweight construction)
    exch_id: str = ""
    src: str = ""
    exch_match_id: str = ""

    @property
    def side_label(self) -> str:
        return _SIDE_LABELS.get(self.side, self.side)

    @property
    def type_label(self) -> str:
        return _TYPE_LABELS.get(self.type, self.type)
