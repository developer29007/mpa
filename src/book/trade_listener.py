from abc import ABC, abstractmethod

from book.trade import Trade


class TradeListener(ABC):

    @abstractmethod
    def on_trade(self, trade: Trade):
        pass
