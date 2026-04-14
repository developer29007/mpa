#!/bin/bash
# Entrypoint for the db-consumer service.
# Reads configuration from environment variables set on the Railway service
# and passes them as arguments to the Python consumer.
set -euo pipefail

: "${ITCH_DATE:?ITCH_DATE environment variable is required}"
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS environment variable is required}"
: "${MPA_DSN:?MPA_DSN environment variable is required}"

echo "[start_db_consumer] Starting db-consumer for date: $ITCH_DATE"
echo "[start_db_consumer] Kafka: $KAFKA_BOOTSTRAP_SERVERS"

exec python -m consumers.db_consumer \
    --date "$ITCH_DATE" \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --dsn "$MPA_DSN"
