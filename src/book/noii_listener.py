from abc import ABC, abstractmethod

from book.noii import Noii


class NoiiListener(ABC):

    @abstractmethod
    def on_noii(self, noii: Noii):
        pass
