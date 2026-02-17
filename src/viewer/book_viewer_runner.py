import argparse
import threading
from datetime import datetime
from pathlib import Path

from itch.itch_feed_handler import ItchFeedHandler
from viewer.book_viewer import BookViewer

'''
Standalone CLI runner for the real-time order book viewer.

Drives the ItchFeedHandler in a background thread while the BookViewer
renders the target stock's order book on the main thread using rich.

Usage:
    python -m viewer.book_viewer_runner --stock AAPL --date 01152026
    python -m viewer.book_viewer_runner --stock AAPL --date 01152026 --depth 20 --price-dec 2
    python -m viewer.book_viewer_runner --stock AAPL --date 01152026 --file ./data/sample.NASDAQ_ITCH50 --refresh 100
'''


def _process_feed(feed_handler: ItchFeedHandler, itch_file_path: Path,
                  max_msgs: int, viewer: BookViewer):
    """Process ITCH file in background thread."""
    batch = 1000
    msg_processed = 0
    try:
        with itch_file_path.open('rb') as data:
            while True:
                if not feed_handler.parser.decode(data, batch):
                    break
                msg_processed += batch
                if max_msgs and msg_processed >= max_msgs:
                    break
    finally:
        viewer.notify_feed_done()


def main():
    parser = argparse.ArgumentParser(
        description='Real-time Order Book Viewer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Press Ctrl+C to exit the viewer.',
    )
    parser.add_argument('--stock', required=True,
                        help='Stock symbol to display (e.g. AAPL)')
    parser.add_argument('--date', required=True,
                        help='Business date MMDDYYYY')
    parser.add_argument('--file', default=None,
                        help='ITCH file path (default: ./data/{date}.NASDAQ_ITCH50)')
    parser.add_argument('--depth', type=int, default=10,
                        help='Price levels per side (default: 10)')
    parser.add_argument('--price-dec', type=int, default=4,
                        help='Price decimal places (default: 4, use 8 for FX)')
    parser.add_argument('--refresh', type=int, default=250,
                        help='Refresh interval in ms (default: 250, min: 50)')
    parser.add_argument('--max-msgs', type=int, default=0,
                        help='Max messages to process (0 = all)')
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, '%m%d%Y').date()
    itch_file = args.file or f'./data/{args.date}.NASDAQ_ITCH50'
    itch_file_path = Path(itch_file)

    if not itch_file_path.exists():
        print(f"Error: ITCH file not found: {itch_file_path}")
        return

    stock = args.stock.strip().upper()

    feed_handler = ItchFeedHandler(trade_date=date_obj)

    viewer = BookViewer(
        stock=stock,
        trade_date=date_obj,
        depth=args.depth,
        price_decimals=args.price_dec,
        refresh_ms=args.refresh,
    )

    # Register viewer as trade listener on feed_handler.
    # This auto-propagates to each OrderBook as it is created.
    feed_handler.register_trade_listener(viewer)

    # Process feed in background thread
    feed_thread = threading.Thread(
        target=_process_feed,
        args=(feed_handler, itch_file_path, args.max_msgs, viewer),
        daemon=True,
    )
    feed_thread.start()

    # Run viewer on main thread — blocks until Ctrl+C
    viewer.run(feed_handler)


if __name__ == '__main__':
    main()
