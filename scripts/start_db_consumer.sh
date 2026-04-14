#!/bin/bash
# Entrypoint for the db-consumer service.
# Reads ITCH_DATE, KAFKA_BOOTSTRAP_SERVERS, and MPA_DSN from the Railway
# service environment and passes them as arguments to the Python consumer.
set -euo pipefail

exec python -m consumers.db_consumer \
    --date "$ITCH_DATE" \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --dsn "$MPA_DSN"
