
from dataclasses import dataclass
from typing import Optional


@dataclass
class TopOfBook:
    name: str
    timestamp: int = 0
    bid_price: Optional[int] = None
    bid_size: int = 0
    ask_price: Optional[int] = None
    ask_size: int = 0
    last_trade: Optional[float] = None
    last_trade_timestamp: int = 0

