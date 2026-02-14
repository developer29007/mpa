import struct

from confluent_kafka import Producer


class KafkaPublisher:

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 5,
            'batch.num.messages': 1000,
        })

    def _publish(self, msg_type: str, payload: bytes):
        msg_size = 1 + len(payload)
        framed = struct.pack('>H', msg_size) + msg_type.encode('ascii') + payload
        self.producer.produce(self.topic, framed)

    def flush(self):
        self.producer.flush()
