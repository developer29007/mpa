"""
Pytest configuration and shared stubs.

confluent_kafka is a native extension that is not installed in the test
environment (no Kafka broker required for unit tests). We stub it out at
the sys.modules level before any test module imports it, so that tests can
patch KafkaPublisher.__init__ / _publish without needing the real library.
"""
import sys
import types

# Build a minimal stub for confluent_kafka so that
#   from confluent_kafka import Producer
# succeeds without the real C extension.
_confluent_kafka_stub = types.ModuleType('confluent_kafka')
_confluent_kafka_stub.Producer = object
sys.modules.setdefault('confluent_kafka', _confluent_kafka_stub)
