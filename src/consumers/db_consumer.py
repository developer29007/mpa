import argparse
import signal
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from consumers.deserializers import deserialize_trade, deserialize_vwap, deserialize_tob, deserialize_noii
from db.connection import connect, ensure_partitions
from db.inserter import DbInserter


def main():
    parser = argparse.ArgumentParser(description="Kafka-to-Postgres consumer for trades, VWAP, and TOB")
    parser.add_argument("--date", required=True, help="Business date MMDDYYYY")
    parser.add_argument("--kafka", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--dsn", required=True, help="Postgres DSN (e.g. postgresql://user:pass@localhost:5432/mpa)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Messages per DB flush (default: 1000)")
    parser.add_argument("--flush-interval", type=float, default=2.0, help="Max seconds between flushes (default: 2.0)")
    args = parser.parse_args()

    trade_date = datetime.strptime(args.date, "%m%d%Y").date()
    trade_date_iso = trade_date.isoformat()

    # Connect to Postgres and ensure partitions exist
    conn = connect(args.dsn)
    ensure_partitions(conn, trade_date)
    inserter = DbInserter(conn, trade_date)
    print(f"Connected to Postgres, partitions ready for {trade_date_iso}")

    # Set up Kafka consumer
    topics = ["trades", "tob", "vwap", "noii"]
    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        "group.id": f"db-consumer-{trade_date_iso}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    print(f"Subscribed to {topics}")

    # Buffers
    trade_buf: list[dict] = []
    vwap_buf: list[dict] = []
    tob_buf: list[dict] = []
    noii_buf: list[dict] = []
    last_flush = time.monotonic()

    # Stats
    total_trades = 0
    total_vwaps = 0
    total_tobs = 0
    total_noii = 0
    total_flushes = 0

    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    def flush():
        nonlocal trade_buf, vwap_buf, tob_buf, noii_buf, last_flush
        nonlocal total_trades, total_vwaps, total_tobs, total_noii, total_flushes
        if not trade_buf and not vwap_buf and not tob_buf and not noii_buf:
            return
        count = len(trade_buf) + len(vwap_buf) + len(tob_buf) + len(noii_buf)
        inserter.flush(trade_buf, vwap_buf, tob_buf, noii_buf)
        consumer.commit(asynchronous=False)
        total_trades += len(trade_buf)
        total_vwaps += len(vwap_buf)
        total_tobs += len(tob_buf)
        total_noii += len(noii_buf)
        total_flushes += 1
        trade_buf = []
        vwap_buf = []
        tob_buf = []
        noii_buf = []
        last_flush = time.monotonic()
        print(f"Flush #{total_flushes}: {count} msgs "
              f"(total: {total_trades} trades, {total_vwaps} vwap, {total_tobs} tob, {total_noii} noii)")

    print("Consuming ...")
    try:
        while running:
            msg = consumer.poll(0.1)
            if msg is None:
                # Check time-based flush even when idle
                if time.monotonic() - last_flush >= args.flush_interval:
                    flush()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            data = msg.value()
            # Framed format: 2-byte size + 1-byte msg_type + payload
            msg_type = chr(data[2])
            payload = data[3:]

            if msg_type == "T":
                trade_buf.append(deserialize_trade(payload))
            elif msg_type == "V":
                vwap_buf.append(deserialize_vwap(payload))
            elif msg_type == "B":
                tob_buf.append(deserialize_tob(payload))
            elif msg_type == "I":
                noii_buf.append(deserialize_noii(payload))

            buf_size = len(trade_buf) + len(vwap_buf) + len(tob_buf) + len(noii_buf)
            if buf_size >= args.batch_size or time.monotonic() - last_flush >= args.flush_interval:
                flush()
    finally:
        flush()
        consumer.close()
        conn.close()
        print(f"Stopped. Total: {total_trades} trades, {total_vwaps} vwap, {total_tobs} tob, "
              f"{total_noii} noii in {total_flushes} flushes")


if __name__ == "__main__":
    main()
