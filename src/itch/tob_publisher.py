from book.top_of_book import TopOfBook


class TopOfBookPublisher:

    def __init__(self, port, host=None):
        self.port = port
        self.host = host
        # TODO: write joiner
        self.joiner = None

    # def send_tob(self, top_of_book: TopOfBook):
    #     # TODO: serialize and send
