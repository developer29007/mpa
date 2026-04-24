import argparse
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from consumers.db_insert_listener import DbInsertListener
from consumers.deserializers import (deserialize_candle, deserialize_market_event,
                                     deserialize_trade, deserialize_vwap,
                                     deserialize_tob, deserialize_noii)
from db.connection import connect, ensure_partitions
from db.inserter import DbInserter


def main():
    parser = argparse.ArgumentParser(description="Kafka-to-Postgres consumer for trades, VWAP, TOB, NOII, and market events")
    parser.add_argument("--date", required=True, help="Business date MMDDYYYY")
    parser.add_argument("--kafka", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--dsn", required=True, help="Postgres DSN (e.g. postgresql://user:pass@localhost:5432/mpa)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Messages per DB flush (default: 1000)")
    parser.add_argument("--flush-interval", type=float, default=2.0, help="Max seconds between flushes (default: 2.0)")
    args = parser.parse_args()

    trade_date = datetime.strptime(args.date, "%m%d%Y").date()
    trade_date_iso = trade_date.isoformat()

    conn = connect(args.dsn)
    ensure_partitions(conn, trade_date)
    inserter = DbInserter(conn, trade_date)
    listener = DbInsertListener(inserter, batch_size=args.batch_size)
    print(f"Connected to Postgres, partitions ready for {trade_date_iso}")

    topics = ["trades", "tob", "vwap", "noii", "market_events", "candles"]
    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        "group.id": f"db-consumer-{trade_date_iso}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    print(f"Subscribed to {topics}")

    last_flush = time.monotonic()
    total_trades = total_vwaps = total_tobs = total_noii = total_market_events = total_candles = total_flushes = 0

    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    def flush():
        nonlocal last_flush, total_trades, total_vwaps, total_tobs, total_noii, total_market_events, total_candles, total_flushes
        trades, vwaps, tobs, noii, market_events, candles = listener.flush()
        count = trades + vwaps + tobs + noii + market_events + candles
        if count == 0:
            return
        consumer.commit(asynchronous=False)
        total_trades += trades
        total_vwaps += vwaps
        total_tobs += tobs
        total_noii += noii
        total_market_events += market_events
        total_candles += candles
        total_flushes += 1
        last_flush = time.monotonic()
        print(f"Flush #{total_flushes}: {count} msgs "
              f"(total: {total_trades} trades, {total_vwaps} vwap, {total_tobs} tob, "
              f"{total_noii} noii, {total_market_events} market_events, {total_candles} candles)")

    print("Consuming ...")
    try:
        while running:
            msg = consumer.poll(0.1)
            if msg is None:
                if time.monotonic() - last_flush >= args.flush_interval:
                    flush()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            data = msg.value()
            msg_type = chr(data[2])
            payload = data[3:]

            if msg_type == "T":
                listener.buffer_trade_dict(deserialize_trade(payload))
            elif msg_type == "V":
                listener.buffer_vwap_dict(deserialize_vwap(payload))
            elif msg_type == "B":
                listener.buffer_tob_dict(deserialize_tob(payload))
            elif msg_type == "I":
                listener.buffer_noii_dict(deserialize_noii(payload))
            elif msg_type == "M":
                listener.buffer_market_event_dict(deserialize_market_event(payload))
            elif msg_type == "C":
                listener.buffer_candle_dict(deserialize_candle(payload))

            if listener.pending_count >= args.batch_size or time.monotonic() - last_flush >= args.flush_interval:
                flush()
    finally:
        flush()
        consumer.close()
        conn.close()
        print(f"Stopped. Total: {total_trades} trades, {total_vwaps} vwap, {total_tobs} tob, "
              f"{total_noii} noii, {total_market_events} market_events, {total_candles} candles "
              f"in {total_flushes} flushes")


if __name__ == "__main__":
    main()
