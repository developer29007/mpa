import argparse
import json
import calendar
import signal
import struct
import sys
from datetime import datetime

from confluent_kafka import Consumer, KafkaError

from publishers.trade_publisher import TRADE_FORMAT
from web.chart_server import ChartServer


def _deserialize_trade(payload: bytes, trade_date=None) -> dict:
    """Deserialize binary trade payload to a dict. Exact inverse of _serialize_trade."""
    _msg_id, ts_ns, sec_id, shares, price, side, trade_type, exch_id, src, match_id = (
        struct.unpack(TRADE_FORMAT, payload)
    )
    if trade_date is not None:
        epoch_seconds = calendar.timegm(trade_date.timetuple()) + ts_ns / 1_000_000_000
    else:
        epoch_seconds = ts_ns / 1_000_000_000
    return {
        "s": sec_id.decode("ascii").strip(),
        "p": price,
        "t": epoch_seconds,
        "v": shares,
        "sd": side.decode("ascii").strip(),
    }


def _trade_dict_to_json(d: dict) -> str:
    return json.dumps(d, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description="Standalone chart server reading from Kafka")
    parser.add_argument("--date", required=True, help="Business date MMDDYYYY")
    parser.add_argument("--kafka", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--stocks", nargs="+", default=None, metavar="STOCK", help="Filter to these stocks")
    parser.add_argument("--port", type=int, default=8765, help="Chart server port (default: 8765)")
    parser.add_argument("--host", default="localhost", help="Chart server host (default: localhost)")
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, "%m%d%Y").date()
    topic = f"trade-analytics-{date_obj.isoformat()}"
    stock_filter = set(args.stocks) if args.stocks else None

    server = ChartServer(host=args.host, port=args.port)
    server.start_background()

    consumer = Consumer({
        "bootstrap.servers": args.kafka,
        # TODO: what's this group.id?
        "group.id": f"chart-runner-{topic}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Consuming from {topic} ...")
    try:
        while running:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue

            data = msg.value()
            # Framed format: 2-byte size + 1-byte msg_type + payload
            # TODO: we are not checking if all the bytes are available.
            msg_type = chr(data[2])
            if msg_type != "T":
                continue
            payload = data[3:]
            trade = _deserialize_trade(payload, date_obj)
            if stock_filter and trade["s"] not in stock_filter:
                continue
            server.enqueue_trade(_trade_dict_to_json(trade))
    finally:
        consumer.close()
        print("Chart runner stopped.")


if __name__ == "__main__":
    main()
