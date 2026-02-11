from abc import ABC, abstractmethod

from book.top_of_book import TopOfBook

'''
Top-of-book change listener.
'''


class TobListener(ABC):

    @abstractmethod
    def on_tob_change(self, tob: TopOfBook):
        pass
