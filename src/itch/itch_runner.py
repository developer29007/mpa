import argparse
from datetime import datetime
from pathlib import Path

from itch.itch_feed_handler import ItchFeedHandler
from util.TimerService import TimerService
from util.TimeUtil import nanos_to_us_str


def main():
    parser = argparse.ArgumentParser(description='ITCH 5.0 feed handler')
    parser.add_argument('--date', required=True, help='Business date MMDDYYYY')
    parser.add_argument('--file', default=None, help='ITCH file path (default: ./data/{date}.NASDAQ_ITCH50)')
    parser.add_argument('--kafka', default=None, help='Kafka bootstrap servers (enables publishers; see --publish)')
    parser.add_argument('--db', default=None, metavar='DSN',
                        help='Postgres DSN for direct DB sink (mutually exclusive with --kafka)')
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
    parser.add_argument('--max-market-time', default=None, metavar='HH:MM:SS',
                        help='Stop processing when ITCH market time reaches this wall-clock time (e.g. 09:31:00)')
    parser.add_argument('--bucket-intervals', nargs='+', type=int,
                        default=[250, 1000, 2000, 5000, 10000, 20000],
                        metavar='MS', help='VWAP bucket intervals in ms')
    parser.add_argument('--trade-bucket-intervals', nargs='+', type=int,
                        default=[60_000, 300_000],
                        metavar='MS', help='Trade bucket intervals in ms (default: 1-min and 5-min)')
    parser.add_argument('--publish', nargs='+',
                        choices=['trades', 'tob', 'vwap', 'noii', 'market_events', 'tradebucket', 'all'],
                        default=['all'],
                        metavar='PUBLISHER',
                        help='Publishers to enable (default: all). '
                             'Choices: trades tob vwap noii market_events tradebucket all')
    args = parser.parse_args()

    if args.kafka and args.db:
        parser.error("--kafka and --db are mutually exclusive")

    date_obj = datetime.strptime(args.date, '%m%d%Y').date()
    itch_file = args.file or f'./data/{args.date}.NASDAQ_ITCH50'
    itch_file_path = Path(itch_file)

    stock_filter = set(args.stocks) if args.stocks else None
    feed_handler = ItchFeedHandler(trade_date=date_obj, stock_filter=stock_filter)

    trade_publisher = None
    tob_publisher = None
    noii_publisher = None
    market_event_publisher = None
    vwap_publishers = []
    trade_bucket_publishers = []

    if args.kafka:
        from confluent_kafka.admin import AdminClient, KafkaException
        from publishers.market_event_publisher import MarketEventPublisher
        from publishers.trade_publisher import TradePublisher
        from publishers.tob_publisher import TobPublisher
        from publishers.vwap_publisher import VwapPublisher
        from publishers.noii_publisher import NoiiPublisher
        from publishers.trade_bucket_publisher import TradeBucketPublisher

        all_publishers = {'trades', 'tob', 'vwap', 'noii', 'market_events', 'tradebucket'}
        publish_set = all_publishers if 'all' in args.publish else set(args.publish)
        print(f"[kafka] Publishing: {sorted(publish_set)}")

        admin = AdminClient({'bootstrap.servers': args.kafka})
        fs = admin.delete_topics(list(all_publishers), operation_timeout=30)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[kafka] Deleted topic: {topic}")
            except KafkaException as e:
                print(f"[kafka] Could not delete topic {topic}: {e}")

        if 'trades' in publish_set:
            trade_publisher = TradePublisher(bootstrap_servers=args.kafka, topic='trades')
            feed_handler.register_trade_listener(trade_publisher)

        if 'tob' in publish_set:
            tob_publisher = TobPublisher(bootstrap_servers=args.kafka, topic='tob')
            feed_handler.register_tob_listener(tob_publisher)

        if 'noii' in publish_set:
            noii_publisher = NoiiPublisher(bootstrap_servers=args.kafka, topic='noii')
            feed_handler.register_noii_listener(noii_publisher)

        if 'market_events' in publish_set:
            market_event_publisher = MarketEventPublisher(bootstrap_servers=args.kafka, topic='market_events')
            feed_handler.register_market_event_listener(market_event_publisher)

        if 'vwap' in publish_set:
            for interval_ms in args.bucket_intervals:
                vwap_pub = VwapPublisher(bootstrap_servers=args.kafka, topic='vwap', interval_ms=interval_ms)
                feed_handler.register_trade_listener(vwap_pub)
                vwap_publishers.append(vwap_pub)

        if 'tradebucket' in publish_set:
            for interval_ms in args.trade_bucket_intervals:
                tb_pub = TradeBucketPublisher(bootstrap_servers=args.kafka, topic='tradebucket', interval_ms=interval_ms)
                feed_handler.register_trade_listener(tb_pub)
                trade_bucket_publishers.append(tb_pub)

    db_listener = None
    if args.db:
        from consumers.db_insert_listener import DbInsertListener
        from db.connection import connect, ensure_partitions
        from db.inserter import DbInserter

        all_data_types = {'trades', 'tob', 'vwap', 'noii', 'market_events', 'tradebucket'}
        publish_set = all_data_types if 'all' in args.publish else set(args.publish)
        print(f"[db] Writing: {sorted(publish_set)}")

        db_conn = connect(args.db)
        ensure_partitions(db_conn, date_obj)
        db_inserter = DbInserter(db_conn, date_obj)
        vwap_intervals = args.bucket_intervals if 'vwap' in publish_set else []
        trade_bucket_intervals = args.trade_bucket_intervals if 'tradebucket' in publish_set else []
        db_listener = DbInsertListener(db_inserter, vwap_interval_ms_list=vwap_intervals,
                                       trade_bucket_interval_ms_list=trade_bucket_intervals)

        if 'trades' in publish_set or 'vwap' in publish_set or 'tradebucket' in publish_set:
            feed_handler.register_trade_listener(db_listener)
        if 'tob' in publish_set:
            feed_handler.register_tob_listener(db_listener)
        if 'noii' in publish_set:
            feed_handler.register_noii_listener(db_listener)
        if 'market_events' in publish_set:
            feed_handler.register_market_event_listener(db_listener)

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

    max_market_time_ns = None
    if args.max_market_time:
        h, m, s = map(int, args.max_market_time.split(':'))
        max_market_time_ns = (h * 3600 + m * 60 + s) * 1_000_000_000
        print(f"Stopping at market time {args.max_market_time} ({max_market_time_ns:,} ns)")

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
                    time_str = nanos_to_us_str(feed_handler.timestamp) if feed_handler.timestamp else '--'
                    print(f"  {msg_processed:>12,} messages  books={len(feed_handler.book_map):,}  market_time={time_str}")

                timer_service.check_timers(feed_handler.timestamp)

                if args.max_msgs and msg_processed >= args.max_msgs:
                    print(f"Reached max messages: {args.max_msgs}")
                    break

                if max_market_time_ns and feed_handler.timestamp and feed_handler.timestamp >= max_market_time_ns:
                    print(f"Reached market time limit {args.max_market_time} at {nanos_to_us_str(feed_handler.timestamp)}")
                    break
    except KeyboardInterrupt:
        print("\nInterrupted.")

    if trade_publisher:
        trade_publisher.flush()
    if tob_publisher:
        tob_publisher.flush()
    if noii_publisher:
        noii_publisher.flush()
    if market_event_publisher:
        market_event_publisher.flush()
    for vwap_pub in vwap_publishers:
        vwap_pub.flush()
    for tb_pub in trade_bucket_publishers:
        tb_pub.flush()

    if db_listener:
        trades, vwaps, tobs, noii, market_events, trade_buckets = db_listener.flush(final=True)
        db_conn.close()
        print(f"[db] Done: {trades} trades, {vwaps} vwap, {tobs} tob, {noii} noii, "
              f"{market_events} market_events, {trade_buckets} trade_buckets")


if __name__ == '__main__':
    main()
