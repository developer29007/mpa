import argparse
from datetime import datetime
from pathlib import Path

from itch.itch_feed_handler import ItchFeedHandler
from util.TimerService import TimerService
from util.TimeUtil import nanos_to_ms_str


def main():
    parser = argparse.ArgumentParser(description='ITCH 5.0 feed handler')
    parser.add_argument('--date', required=True, help='Business date MMDDYYYY')
    parser.add_argument('--file', default=None, help='ITCH file path (default: ./data/{date}.NASDAQ_ITCH50)')
    parser.add_argument('--kafka', default=None, help='Kafka bootstrap servers (enables trade + vwap publishers)')
    parser.add_argument('--print-trades', nargs='+', default=None, metavar='STOCK', help='Stocks to print trades for')
    parser.add_argument('--print-vwap', nargs='+', default=None, metavar='STOCK', help='Stocks to print VWAP for')
    parser.add_argument('--chart', nargs='*', default=None, metavar='STOCK',
                        help='Start trade chart server (optionally filter stocks; omit stocks for all)')
    parser.add_argument('--chart-port', type=int, default=8765, help='Trade chart server port (default: 8765)')
    parser.add_argument('--chart-host', default='localhost',
                        help='Trade chart server host (default: localhost; use 0.0.0.0 to allow network access)')
    parser.add_argument('--candle-chart', nargs='*', default=None, metavar='STOCK',
                        help='Start candle chart server (optionally filter stocks; omit stocks for all)')
    parser.add_argument('--candle-port', type=int, default=8766, help='Candle chart server port (default: 8766)')
    parser.add_argument('--candle-host', default='localhost',
                        help='Candle chart server host (default: localhost; use 0.0.0.0 to allow network access)')
    parser.add_argument('--candle-interval', type=int, default=60, metavar='SECONDS',
                        help='Candle interval in seconds (default: 60)')
    parser.add_argument('--stocks', nargs='+', default=None, metavar='STOCK',
                        help='Only process these stocks (default: all stocks)')
    parser.add_argument('--max-msgs', type=int, default=0, help='Max messages to process (0 = all)')
    parser.add_argument('--bucket-intervals', nargs='+', type=int,
                        default=[250, 1000, 2000, 5000, 10000, 20000],
                        metavar='MS', help='VWAP bucket intervals in ms')
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, '%m%d%Y').date()
    itch_file = args.file or f'./data/{args.date}.NASDAQ_ITCH50'
    itch_file_path = Path(itch_file)

    stock_filter = set(args.stocks) if args.stocks else None
    feed_handler = ItchFeedHandler(trade_date=date_obj, stock_filter=stock_filter)

    trade_publisher = None
    tob_publisher = None
    market_event_publisher = None
    vwap_publishers = []

    if args.kafka:
        from confluent_kafka.admin import AdminClient, KafkaException
        from publishers.market_event_publisher import MarketEventPublisher
        from publishers.trade_publisher import TradePublisher
        from publishers.tob_publisher import TobPublisher
        from publishers.vwap_publisher import VwapPublisher

        topics = ['trades', 'tob', 'vwap', 'market_events']
        admin = AdminClient({'bootstrap.servers': args.kafka})
        fs = admin.delete_topics(topics, operation_timeout=30)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[kafka] Deleted topic: {topic}")
            except KafkaException as e:
                print(f"[kafka] Could not delete topic {topic}: {e}")

        trade_publisher = TradePublisher(bootstrap_servers=args.kafka, topic='trades')
        feed_handler.register_trade_listener(trade_publisher)

        tob_publisher = TobPublisher(bootstrap_servers=args.kafka, topic='tob')
        feed_handler.register_tob_listener(tob_publisher)

        market_event_publisher = MarketEventPublisher(bootstrap_servers=args.kafka, topic='market_events')
        feed_handler.register_market_event_listener(market_event_publisher)

        for interval_ms in args.bucket_intervals:
            vwap_pub = VwapPublisher(bootstrap_servers=args.kafka, topic='vwap', interval_ms=interval_ms)
            feed_handler.register_trade_listener(vwap_pub)
            vwap_publishers.append(vwap_pub)

    if args.print_trades:
        from itch.trade_printer import TradePrinter
        stocks = None if '*' in args.print_trades else set(args.print_trades)
        feed_handler.register_trade_listener(TradePrinter(stocks))

    if args.print_vwap:
        from itch.vwap_printer import VwapPrinter
        stocks = None if '*' in args.print_vwap else set(args.print_vwap)
        feed_handler.register_trade_listener(VwapPrinter(stocks, args.bucket_intervals))

    if args.chart is not None:
        from web.trade_chart_listener import TradeChartListener
        stock_set = None if not args.chart or '*' in args.chart else set(args.chart)
        feed_handler.register_trade_listener(TradeChartListener(stocks=stock_set, host=args.chart_host, port=args.chart_port))

    if args.candle_chart is not None:
        from web.candle_chart_listener import CandleChartListener
        stock_set = None if not args.candle_chart or '*' in args.candle_chart else set(args.candle_chart)
        feed_handler.register_trade_listener(CandleChartListener(
            stocks=stock_set, host=args.candle_host, port=args.candle_port, interval_seconds=args.candle_interval,
        ))

    timer_service = TimerService.instance()
    batch = 1000
    msg_processed = 0
    report_interval = 1_000_000
    print(f"Processing {itch_file_path} ...")
    try:
        with itch_file_path.open('rb') as data:
            while True:
                if not feed_handler.parser.decode(data, batch):
                    print(f"Finished processing {itch_file_path} — {msg_processed:,} messages")
                    break
                msg_processed += batch

                if msg_processed % report_interval == 0:
                    time_str = nanos_to_ms_str(feed_handler.timestamp) if feed_handler.timestamp else '--'
                    print(f"  {msg_processed:>12,} messages  books={len(feed_handler.book_map):,}  market_time={time_str}")

                timer_service.check_timers(feed_handler.timestamp)

                if args.max_msgs and msg_processed >= args.max_msgs:
                    print(f"Reached max messages: {args.max_msgs}")
                    break
    except KeyboardInterrupt:
        print("\nInterrupted.")

    if trade_publisher:
        trade_publisher.flush()
    if tob_publisher:
        tob_publisher.flush()
    if market_event_publisher:
        market_event_publisher.flush()
    for vwap_pub in vwap_publishers:
        vwap_pub.flush()


if __name__ == '__main__':
    main()
