import datetime
from dataclasses import dataclass
from typing import Optional

# Execution
TRADE_TYPE_EXECUTION = "E"
# Execution with Price
TRADE_TYPE_EXECUTION_WITH_PRICE = "P"
# Non Displayable Trade
TRADE_TYPE_NON_CROSS = "N"
# Open Cross
TRADE_TYPE_OPEN_CROSS = "O"
# Close Cross
TRADE_TYPE_CLOSE_CROSS = "C"
# IPO or Halt/Paused
TRADE_TYPE_IPO_OR_HALT = "P"
# UNKNOWN
TRADE_TYPE_UNKNOWN = "U"



@dataclass
class Trade:
    # Required trade data
    timestamp_ns: int
    sec_id: str
    shares: int
    price: float
    side: str
    type: str
    # Optional metadata (defaults for lightweight construction)
    exch_id: str = ""
    src: str = ""
    exch_match_id: str = ""
    trade_date: Optional[datetime.date] = None
