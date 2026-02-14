import argparse
from datetime import datetime
from pathlib import Path

from itch.itch_feed_handler import ItchFeedHandler


def main():
    parser = argparse.ArgumentParser(description='ITCH 5.0 feed handler')
    parser.add_argument('--date', required=True, help='Business date MMDDYYYY')
    parser.add_argument('--file', default=None, help='ITCH file path (default: ./data/{date}.NASDAQ_ITCH50)')
    parser.add_argument('--kafka', default=None, help='Kafka bootstrap servers (enables trade + vwap publishers)')
    parser.add_argument('--print-trades', nargs='+', default=None, metavar='STOCK', help='Stocks to print trades for')
    parser.add_argument('--print-vwap', nargs='+', default=None, metavar='STOCK', help='Stocks to print VWAP for')
    parser.add_argument('--max-msgs', type=int, default=0, help='Max messages to process (0 = all)')
    parser.add_argument('--bucket-intervals', nargs='+', type=int,
                        default=[250, 1000, 2000, 5000, 10000, 20000],
                        metavar='MS', help='VWAP bucket intervals in ms')
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, '%m%d%Y').date()
    itch_file = args.file or f'./data/{args.date}.NASDAQ_ITCH50'
    itch_file_path = Path(itch_file)

    feed_handler = ItchFeedHandler(trade_date=date_obj)

    trade_publisher = None
    vwap_publisher = None

    if args.kafka:
        from publishers.trade_publisher import TradePublisher
        from publishers.vwap_publisher import VwapPublisher

        trade_publisher = TradePublisher(bootstrap_servers=args.kafka, trade_date=date_obj)
        vwap_publisher = VwapPublisher(bootstrap_servers=args.kafka, trade_date=date_obj,
                                       bucket_intervals=args.bucket_intervals)
        feed_handler.register_trade_listener(trade_publisher)
        feed_handler.register_trade_listener(vwap_publisher)

    if args.print_trades:
        from itch.trade_printer import TradePrinter
        feed_handler.register_trade_listener(TradePrinter(set(args.print_trades)))

    if args.print_vwap:
        from itch.vwap_printer import VwapPrinter
        feed_handler.register_trade_listener(VwapPrinter(set(args.print_vwap), args.bucket_intervals))

    batch = 1000
    msg_processed = 0
    print(f"Processing {itch_file_path} ...")
    with itch_file_path.open('rb') as data:
        while True:
            if not feed_handler.parser.decode(data, batch):
                print(f"Finished processing {itch_file_path}")
                break
            msg_processed += batch
            if args.max_msgs and msg_processed >= args.max_msgs:
                print(f"Reached max messages: {args.max_msgs}")
                break

    if trade_publisher:
        trade_publisher.flush()
    if vwap_publisher:
        vwap_publisher.flush()


if __name__ == '__main__':
    main()
