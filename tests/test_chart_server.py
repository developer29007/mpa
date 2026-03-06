from queue import Empty

from web.chart_server import ChartServer


class TestChartServerQueue:
    def test_enqueue_trade_is_nonblocking(self):
        server = ChartServer()
        server.enqueue_trade('{"s":"AAPL","p":150.0}')
        server.enqueue_trade('{"s":"TSLA","p":200.0}')
        # Should not block; items are in the queue
        assert server._queue.qsize() == 2

    def test_drain_queue_returns_all_items(self):
        server = ChartServer()
        server.enqueue_trade('{"s":"AAPL","p":150.0}')
        server.enqueue_trade('{"s":"TSLA","p":200.0}')
        server.enqueue_trade('{"s":"GOOG","p":100.0}')

        items = server._drain_queue()
        assert len(items) == 3
        assert '{"s":"AAPL","p":150.0}' in items
        assert '{"s":"TSLA","p":200.0}' in items
        assert '{"s":"GOOG","p":100.0}' in items

    def test_drain_queue_empty_returns_empty_list(self):
        server = ChartServer()
        items = server._drain_queue()
        assert items == []

    def test_drain_queue_clears_queue(self):
        server = ChartServer()
        server.enqueue_trade('{"s":"AAPL","p":150.0}')
        server._drain_queue()
        # Queue should be empty after drain
        items = server._drain_queue()
        assert items == []

    def test_multiple_drain_cycles(self):
        server = ChartServer()
        server.enqueue_trade('{"s":"AAPL","p":150.0}')
        batch1 = server._drain_queue()
        assert len(batch1) == 1

        server.enqueue_trade('{"s":"TSLA","p":200.0}')
        server.enqueue_trade('{"s":"GOOG","p":100.0}')
        batch2 = server._drain_queue()
        assert len(batch2) == 2
