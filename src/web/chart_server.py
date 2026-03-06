import asyncio
import threading
from pathlib import Path
from queue import SimpleQueue, Empty

import websockets
from websockets.asyncio.server import serve, ServerConnection
from websockets.datastructures import Headers
from websockets.http11 import Response


class ChartServer:

    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self._queue: SimpleQueue[str] = SimpleQueue()
        self._clients: set[ServerConnection] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._html = (Path(__file__).parent / "trade_chart.html").read_text()

    def enqueue_trade(self, trade_json: str):
        """Thread-safe, non-blocking enqueue of a JSON trade string."""
        self._queue.put_nowait(trade_json)

    def start_background(self):
        """Start the asyncio event loop in a daemon thread."""
        thread = threading.Thread(target=self.run_forever, daemon=True)
        thread.start()

    def run_forever(self):
        """Block on the asyncio event loop (for standalone mode)."""
        asyncio.run(self._run())

    async def _run(self):
        self._loop = asyncio.get_running_loop()
        async with serve(
            self._handler,
            self.host,
            self.port,
            process_request=self._process_request,
        ):
            print(f"Chart server running at http://{self.host}:{self.port}/")
            await self._broadcast_loop()

    async def _process_request(self, connection: ServerConnection, request):
        if request.path == "/" and request.headers.get("Upgrade") is None:
            headers = Headers([("Content-Type", "text/html; charset=utf-8")])
            return Response(200, "OK", headers, self._html.encode())

    async def _handler(self, websocket: ServerConnection):
        self._clients.add(websocket)
        try:
            async for _ in websocket:
                pass  # we don't expect messages from clients
        finally:
            self._clients.discard(websocket)

    async def _broadcast_loop(self):
        while True:
            await asyncio.sleep(0.05)  # 50ms batch interval
            batch = self._drain_queue()
            if batch and self._clients:
                message = "[" + ",".join(batch) + "]"
                websockets.broadcast(self._clients, message)

    def _drain_queue(self) -> list[str]:
        items = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except Empty:
                break
        return items
