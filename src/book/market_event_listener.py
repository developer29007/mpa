from abc import ABC, abstractmethod

from book.market_event import MarketEvent


class MarketEventListener(ABC):
    """Interface for receiving market events: auctions, halts, resumes, and circuit breakers."""

    @abstractmethod
    def on_market_event(self, event: MarketEvent) -> None:
        pass
