#!/bin/bash
# Entrypoint for the db-consumer service.
# Reads ITCH_DATE, KAFKA_BOOTSTRAP_SERVERS, and MPA_DSN from the environment
# and passes them as explicit arguments to the Python consumer, avoiding
# shell variable-expansion issues with Railway's ${{ VAR }} start-command syntax.
set -euo pipefail

ITCH_DATE="${ITCH_DATE:-10302019}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
MPA_DSN="${MPA_DSN:-postgresql://mpa:mpa@localhost:5432/mpa}"

echo "[start_db_consumer] Date:   $ITCH_DATE"
echo "[start_db_consumer] Kafka:  $KAFKA_BOOTSTRAP_SERVERS"
echo "[start_db_consumer] DSN:    $MPA_DSN"

exec python -m consumers.db_consumer \
    --date "$ITCH_DATE" \
    --kafka "$KAFKA_BOOTSTRAP_SERVERS" \
    --dsn "$MPA_DSN"
