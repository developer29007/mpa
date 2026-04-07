#!/bin/bash
# Entrypoint for the itch-runner service.
# On first run with an empty volume, downloads the ITCH file from NASDAQ and decompresses it.
# Subsequent runs skip the download since the file is already present on the volume.
set -euo pipefail

ITCH_DATE="${ITCH_DATE:-10302019}"
DATA_DIR="${DATA_DIR:-/app/data}"
ITCH_FILE="${DATA_DIR}/${ITCH_DATE}.NASDAQ_ITCH50"
ITCH_GZ="${ITCH_FILE}.gz"
NASDAQ_URL="https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/${ITCH_DATE}.NASDAQ_ITCH50.gz"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

mkdir -p "$DATA_DIR"

if [ ! -f "$ITCH_FILE" ]; then
    if [ -f "$ITCH_GZ" ]; then
        echo "[start_itch] Found compressed file, decompressing..."
        gunzip "$ITCH_GZ"
    else
        echo "[start_itch] ITCH file not found. Downloading from NASDAQ..."
        echo "[start_itch] URL: $NASDAQ_URL"
        echo "[start_itch] Destination: $ITCH_GZ"
        curl -L --progress-bar -o "$ITCH_GZ" "$NASDAQ_URL"
        echo "[start_itch] Download complete. Decompressing (~10 GB)..."
        gunzip "$ITCH_GZ"
    fi
    echo "[start_itch] ITCH file ready: $ITCH_FILE"
fi

# Build the command. --stocks accepts one or more symbols; leave ITCH_STOCKS unset for all stocks.
CMD=(python -m itch.itch_runner
    --date "$ITCH_DATE"
    --file "$ITCH_FILE"
    --kafka "$KAFKA_BOOTSTRAP_SERVERS"
)

if [ -n "${ITCH_STOCKS:-}" ]; then
    # ITCH_STOCKS should be space-separated, e.g. "AAPL TSLA MSFT"
    # shellcheck disable=SC2086
    CMD+=(--stocks $ITCH_STOCKS)
fi

echo "[start_itch] Running: ${CMD[*]}"
exec "${CMD[@]}"
