import struct

from confluent_kafka import Producer


def _on_delivery(err, msg):
    """Kafka delivery callback invoked by the producer for each produced message.

    Args:
        err: A KafkaError instance if delivery failed, or None on success.
        msg: The Message object representing the produced message.

    Raises:
        RuntimeError: If the message was not successfully delivered to Kafka.
    """
    if err:
        raise RuntimeError(f"Kafka delivery failed for topic {msg.topic()}: {err}")


class KafkaPublisher:

    def __init__(self, bootstrap_servers: str, topic: str):
        """Initialize the KafkaPublisher with a Confluent producer configured for batched delivery.

        Args:
            bootstrap_servers (str): Comma-separated list of Kafka broker addresses (e.g. 'localhost:9092').
            topic (str): Kafka topic to which messages will be published.
        """
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 5,
            'batch.num.messages': 1000,
        })

    def _publish(self, msg_type: str, payload: bytes):
        """Frame and enqueue a typed message for asynchronous delivery to the Kafka topic.

        Args:
            msg_type (str): Single ASCII character identifying the message type (e.g. 'T' for trade).
            payload (bytes): Raw binary payload to send after the message-type byte.

        Raises:
            RuntimeError: Raised asynchronously via the delivery callback if Kafka fails to deliver the message.
        """
        msg_size = 1 + len(payload)
        framed = struct.pack('>H', msg_size) + msg_type.encode('ascii') + payload
        # on_delivery is invoked by the producer thread once Kafka acknowledges (or rejects) the message
        self.producer.produce(self.topic, framed, on_delivery=_on_delivery)
        # poll(0) is non-blocking call that processes any pending delivery callbacks
        self.producer.poll(0)

    def flush(self):
        """Block until all queued messages are delivered or the timeout expires.

        Returns:
            None

        Raises:
            RuntimeError: If the flush times out with undelivered messages remaining.
        """
        remaining = self.producer.flush(timeout=10)
        if remaining:
            raise RuntimeError(f"Kafka flush timed out with {remaining} message(s) undelivered")
