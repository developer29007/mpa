import datetime
from dataclasses import dataclass


@dataclass
class Noii:
    timestamp_ns: int
    stock: str
    paired_shares: int
    imbalance_shares: int
    imbalance_direction: str    # 'B'=Buy, 'S'=Sell, 'N'=No imbalance, 'O'=Insufficient orders
    far_price: float | None     # None if raw price == 0 (not applicable)
    near_price: float | None    # None if raw price == 0 (not applicable)
    current_reference_price: float
    cross_type: str             # 'O'=Opening, 'C'=Closing, 'H'=IPO/Halt, 'A'=Extended
    price_variation_indicator: str  # 'L'=<1%, '1'-'9'=1%-9.99%, 'A'=10-19.99%, 'B'=20-29.99%, 'C'=>=30%, ' '=N/A
    trade_date: datetime.date
