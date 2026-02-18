import datetime
import time
from itertools import zip_longest
from typing import Optional

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from book.order_book import OrderBook
from book.trade import Trade
from book.trade_listener import TradeListener
from util.TimeUtil import nanos_to_ms_str

'''
Real-time terminal order book viewer.

Implements TradeListener to track trade activity internally.
Discovers the target OrderBook from the ItchFeedHandler's book_map
and reads its state for rendering with the rich library.

Usage:
    See book_viewer_runner.py for CLI entry point.
'''


class BookViewer(TradeListener):

    CONTENT_WIDTH = 72  # chars inside panel border + padding

    def __init__(self, stock: str, trade_date: datetime.date,
                 depth: int = 10, price_decimals: int = 4, refresh_ms: int = 250):
        self._stock = stock
        self._display_name = stock.strip()
        # Match both stripped and 8-char padded forms (ITCH pads to 8)
        self._stock_match: set[str] = {stock, stock.ljust(8)}
        self._trade_date = trade_date
        self._depth = depth
        self._price_dec = price_decimals
        self._refresh_ms = max(refresh_ms, 50)

        self._book: Optional[OrderBook] = None
        self._running = True
        self._feed_done = False

        # Trade tracking — maintained by on_trade callbacks
        self._last_trade: Optional[Trade] = None
        self._opening_trade: Optional[Trade] = None
        self._total_volume: int = 0

        # Book discovery timestamp
        self._started_ns: int = 0

        # Market status — placeholder for future use
        self._market_status = "\u2014"

    # ── TradeListener interface ────────────────────────────────────

    def on_trade(self, trade: Trade):
        if trade.sec_id not in self._stock_match and trade.sec_id.strip() not in self._stock_match:
            return
        self._last_trade = trade
        if self._opening_trade is None:
            self._opening_trade = trade
        self._total_volume += trade.shares

    # ── Lifecycle ──────────────────────────────────────────────────

    def notify_feed_done(self):
        self._feed_done = True

    def stop(self):
        self._running = False

    # ── Formatting helpers ─────────────────────────────────────────

    def _fp(self, price_raw: int) -> str:
        """Format raw price (int × 10000) to decimal string."""
        return f"{price_raw / 10000:.{self._price_dec}f}"

    def _fpd(self, price_float: float) -> str:
        """Format dollar price with $ prefix."""
        return f"${price_float:.{self._price_dec}f}"

    def _fs(self, size: int) -> str:
        """Format size with comma separators."""
        return f"{size:,}"

    def _ft(self, nanos: int) -> str:
        """Format nanosecond timestamp to HH:MM:SS.mmm."""
        return nanos_to_ms_str(nanos) if nanos else "\u2014"

    # ── Section builders ───────────────────────────────────────────

    def _build_header(self) -> Text:
        """Section 1: Stock, status badge, date, started time."""
        date_str = self._trade_date.strftime('%m/%d/%Y')
        started_str = self._ft(self._started_ns)
        feed_tag = " [DONE]" if self._feed_done else ""

        W = self.CONTENT_WIDTH
        left1 = f"  {self._display_name} Order Book{feed_tag}"
        right1 = f"Date: {date_str}"
        gap1 = max(W - len(left1) - len(right1), 2)

        left2 = f"  Status: [{self._market_status}]"
        right2 = f"Started: {started_str}"
        gap2 = max(W - len(left2) - len(right2), 2)

        t = Text()
        t.append(f"  {self._display_name}", style="bold white")
        t.append(" Order Book", style="white")
        if feed_tag:
            t.append(feed_tag, style="dim yellow")
        t.append(" " * gap1)
        t.append(f"{right1}\n", style="dim")

        t.append("  Status: ", style="dim")
        t.append(f"[{self._market_status}]", style="dim")
        t.append(" " * gap2)
        t.append(right2, style="dim")

        return t

    def _build_trades(self) -> Text:
        """Section 2: Last trade + opening trade."""
        t = Text()

        lt = self._last_trade
        if lt:
            t.append("  Last Trade      ", style="dim")
            t.append(self._fpd(lt.price), style="bold white")
            t.append(f"  \u00d7 {lt.shares:,}", style="white")
            if lt.side:
                t.append(f"   Side: {lt.side}", style="white")
            t.append(f"   @ {self._ft(lt.timestamp_ns)}", style="dim")
        else:
            t.append("  Last Trade      \u2014", style="dim")

        t.append("\n")

        ot = self._opening_trade
        if ot:
            t.append("  Opening Trade   ", style="dim")
            t.append(self._fpd(ot.price), style="bold white")
            t.append(f"  \u00d7 {ot.shares:,}", style="white")
            t.append(f"   @ {self._ft(ot.timestamp_ns)}", style="dim")
        else:
            t.append("  Opening Trade   \u2014", style="dim")

        return t

    def _build_summary(self) -> Text:
        """Section 3: Spread, bid/ask ranges, level counts."""
        t = Text()
        book = self._book

        if not book:
            t.append("  Spread: \u2014", style="dim")
            return t

        # Spread
        try:
            if book.bids and book.asks:
                best_bid_px = book.bids.peekitem(-1)[0]
                best_ask_px = book.asks.peekitem(0)[0]
                spread = (best_ask_px - best_bid_px) / 10000
                t.append("  Spread: ", style="dim")
                t.append(f"${spread:.{self._price_dec}f}", style="bold yellow")
            else:
                t.append("  Spread: \u2014", style="dim")
        except (IndexError, KeyError, RuntimeError):
            t.append("  Spread: \u2014", style="dim")

        t.append("\n")

        # Bid range (filter stale levels with size <= 0)
        try:
            live_bid_levels = [k for k, v in reversed(book.bids.items()) if v.size > 0]
            total_bid = len(live_bid_levels)
            shown_bid = min(self._depth, total_bid)
            if live_bid_levels and shown_bid > 0:
                t.append("  Bid Range: ", style="dim")
                t.append(f"{self._fp(live_bid_levels[shown_bid - 1])} \u2013 {self._fp(live_bid_levels[0])}", style="white")
                t.append(f"  ({shown_bid} / {total_bid} levels)", style="dim")
            else:
                t.append("  Bid Range: \u2014", style="dim")
        except (IndexError, RuntimeError):
            t.append("  Bid Range: \u2014", style="dim")

        t.append("\n")

        # Ask range (filter stale levels with size <= 0)
        try:
            live_ask_levels = [k for k, v in book.asks.items() if v.size > 0]
            total_ask = len(live_ask_levels)
            shown_ask = min(self._depth, total_ask)
            if live_ask_levels and shown_ask > 0:
                t.append("  Ask Range: ", style="dim")
                t.append(f"{self._fp(live_ask_levels[0])} \u2013 {self._fp(live_ask_levels[shown_ask - 1])}", style="white")
                t.append(f"  ({shown_ask} / {total_ask} levels)", style="dim")
            else:
                t.append("  Ask Range: \u2014", style="dim")
        except (IndexError, RuntimeError):
            t.append("  Ask Range: \u2014", style="dim")

        return t

    def _build_book_table(self) -> Table:
        """Section 4: The bid/ask depth grid."""
        pw = self._price_dec + 6  # e.g. "150.4900" = 8 chars for 4 dec

        table = Table(
            show_header=True, header_style="dim",
            box=None, pad_edge=True, padding=(0, 1), show_edge=False,
        )
        table.add_column("#Ord", justify="right", width=5, style="dim")
        table.add_column("Size", justify="right", width=10)
        table.add_column("Bid", justify="right", width=pw)
        table.add_column("", justify="center", width=1)
        table.add_column("Ask", justify="left", width=pw)
        table.add_column("Size", justify="left", width=10)
        table.add_column("#Ord", justify="left", width=5, style="dim")

        if not self._book or (not self._book.bids and not self._book.asks):
            table.add_row("", "", "", "\u2502", "", "", "")
            return table

        try:
            # Filter out stale levels with size <= 0 (can appear briefly due to
            # the feed thread reducing size before cleanup removes the level)
            bid_levels = [lvl for lvl in reversed(self._book.bids.values()) if lvl.size > 0][:self._depth]
            ask_levels = [lvl for lvl in self._book.asks.values() if lvl.size > 0][:self._depth]
        except RuntimeError:
            # SortedDict modified during iteration by feed thread
            return table

        for i, (bid, ask) in enumerate(zip_longest(bid_levels, ask_levels)):
            b_ord = str(len(bid.orders)) if bid else ""
            b_sz = self._fs(bid.size) if bid else ""
            b_px = self._fp(bid.price) if bid else ""

            a_ord = str(len(ask.orders)) if ask else ""
            a_sz = self._fs(ask.size) if ask else ""
            a_px = self._fp(ask.price) if ask else ""

            style = "bold" if i == 0 else ""
            table.add_row(b_ord, b_sz, b_px, "\u2502", a_px, a_sz, a_ord, style=style)

        return table

    def _build_footer(self) -> Text:
        """Section 5: Total depths, order count, volume, updated timestamp."""
        t = Text()
        book = self._book
        W = self.CONTENT_WIDTH

        try:
            bid_depth = sum(lvl.size for lvl in book.bids.values() if lvl.size > 0) if book else 0
            ask_depth = sum(lvl.size for lvl in book.asks.values() if lvl.size > 0) if book else 0
            total_orders = (
                sum(len(lvl.orders) for lvl in book.bids.values() if lvl.size > 0) +
                sum(len(lvl.orders) for lvl in book.asks.values() if lvl.size > 0)
            ) if book else 0
            updated_str = self._ft(book.timestamp_ns) if book else "\u2014"
        except RuntimeError:
            bid_depth = ask_depth = total_orders = 0
            updated_str = "\u2014"

        # Line 1
        t.append("  Bid Depth: ", style="dim")
        t.append(f"{bid_depth:,}", style="white")
        t.append("    Ask Depth: ", style="dim")
        t.append(f"{ask_depth:,}", style="white")
        t.append("    Orders: ", style="dim")
        t.append(f"{total_orders:,}", style="white")
        t.append("\n")

        # Line 2
        vol_str = f"  Volume: {self._total_volume:,}"
        upd_str = f"Updated: {updated_str}"
        gap = max(W - len(vol_str) - len(upd_str), 2)

        t.append("  Volume: ", style="dim")
        t.append(f"{self._total_volume:,}", style="white")
        t.append(" " * gap)
        t.append(upd_str, style="dim")

        return t

    # ── Main display ───────────────────────────────────────────────

    def _build_display(self) -> Panel:
        """Assemble all sections into a single Panel."""
        if self._book is None:
            if self._feed_done:
                msg = Text(f"\n  No orders found for {self._display_name}.\n", style="bold red")
            else:
                msg = Text(f"\n  Waiting for {self._display_name} orders...\n", style="dim italic")
            return Panel(msg, border_style="dim", width=self.CONTENT_WIDTH + 4)

        return Panel(
            Group(
                self._build_header(),
                Rule(style="dim"),
                self._build_trades(),
                Rule(style="dim"),
                self._build_summary(),
                Rule(style="dim"),
                self._build_book_table(),
                Rule(style="dim"),
                self._build_footer(),
            ),
            border_style="dim",
            width=self.CONTENT_WIDTH + 4,
        )

    # ── Run loop ───────────────────────────────────────────────────

    def run(self, feed_handler):
        """Main display loop. Blocks until Ctrl+C or feed completes + user exits."""
        console = Console()

        with Live(self._build_display(), console=console,
                  screen=True, auto_refresh=False) as live:
            try:
                while self._running:
                    # Discover order book once it exists in feed_handler
                    if self._book is None:
                        book = feed_handler.book_map.get(self._stock)
                        if book is None:
                            book = feed_handler.book_map.get(self._stock.ljust(8))
                        if book:
                            self._book = book
                            self._stock = book.stock
                            self._stock_match.add(book.stock)
                            self._started_ns = book.timestamp_ns

                    live.update(self._build_display(), refresh=True)
                    time.sleep(self._refresh_ms / 1000)
            except KeyboardInterrupt:
                pass

        # Print final snapshot to normal terminal after exiting alt-screen
        console.print(self._build_display())
