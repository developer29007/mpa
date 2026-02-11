from dataclasses import dataclass
from typing import ClassVar


def __post_init__():
    Order.count += 1


@dataclass
class Order:
    count: ClassVar[int]
    id: int
    timestamp_ns: int
    stock: str
    buy_sell: str
    shares: int
    price: int

    def is_bid(self):
        return True if self.buy_sell == 'B' else False
