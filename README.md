# MPA — Market Participant Analytics

A Python toolkit for parsing NASDAQ ITCH 5.0 binary feed data, maintaining a real-time order book, computing VWAP analytics, and visualising trades via interactive browser charts.

---

## Features

- **ITCH 5.0 parser** — decodes the full NASDAQ binary protocol
- **Order book** — maintains live bid/offer levels per stock
- **Trade printer** — prints real-time trades and VWAP to stdout
- **Trade chart** — live line chart of individual trades streamed over WebSocket (port 8765)
- **Candle chart** — live OHLCV candlestick chart with bid/offer volume split (port 8766)
- **Kafka publisher** — publishes trade and VWAP events to Kafka topics (optional)

---

## Prerequisites

### Python 3.12

The project requires **Python 3.12** exactly. Check your version:

```bash
python3 --version
```

If you need to install Python 3.12, the recommended approach is [`pyenv`](https://github.com/pyenv/pyenv):

**macOS / Linux**
```bash
# Install pyenv
curl https://pyenv.run | bash

# Add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# Install and activate Python 3.12
pyenv install 3.12.8
pyenv global 3.12.8
python3 --version   # should print Python 3.12.x
```

**Windows** — download the installer from https://www.python.org/downloads/release/python-3128/ and ensure "Add Python to PATH" is checked.

### PDM (package manager)

```bash
pip install pdm
```

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/developer29007/mpa.git
cd mpa

# 2. Install dependencies (creates .venv automatically)
pdm install
```

This installs:
- `sortedcontainers` — efficient sorted order book data structures
- `confluent-kafka` — Kafka publisher support (optional at runtime)
- `websockets` — WebSocket server for the browser charts

To run commands, prefix with `pdm run` or activate the virtual environment first:

```bash
# Option A — prefix every command
pdm run python -m itch.itch_runner --help

# Option B — activate venv once, then run normally
source .venv/bin/activate          # macOS/Linux
.venv\Scripts\activate             # Windows
python -m itch.itch_runner --help
```

---

## Download ITCH Data

NASDAQ publishes historical ITCH 5.0 binary files at:

**https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/**

Files are listed by date in the format `MMDDYYYY.NASDAQ_ITCH50.gz`. A single day's file is typically 5–10 GB uncompressed, so pick a recent date.

```bash
# Create the data directory (it is gitignored)
mkdir -p data

# Example: download January 30 2019
curl -O https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz

# Decompress (requires ~8 GB free space)
gunzip 01302019.NASDAQ_ITCH50.gz

# Move to the data directory
mv 01302019.NASDAQ_ITCH50 data/
```

The runner expects the file at `./data/{MMDDYYYY}.NASDAQ_ITCH50` by default. You can override the path with `--file`.

---

## Running

All examples below assume you are in the repo root and have activated the virtual environment (or prefix with `pdm run`).

Replace `01302019` with the date of the file you downloaded.

### Print trades to stdout

```bash
# Print every AAPL and TSLA trade as it is parsed
python -m itch.itch_runner --date 01302019 --print-trades AAPL TSLA
```

### Print VWAP to stdout

```bash
# Print rolling VWAP for AAPL at 1s, 5s, and 10s intervals
python -m itch.itch_runner --date 01302019 --print-vwap AAPL --bucket-intervals 1000 5000 10000
```

### Live trade chart (line chart, individual trades)

```bash
python -m itch.itch_runner --date 01302019 --chart AAPL TSLA
```

Then open **http://localhost:8765** in your browser. The chart streams every trade in real time via WebSocket.

- Omit the stock list to chart all stocks: `--chart`
- Change the port: `--chart-port 9000`

### Live candle chart (OHLCV + bid/offer volume)

```bash
python -m itch.itch_runner --date 01302019 --candle-chart AAPL TSLA --candle-interval 60
```

Then open **http://localhost:8766** in your browser.

- Each candle covers `--candle-interval` seconds (default: 60)
- The volume panel below each candlestick shows the bid/offer split:
  - Green = offer-side volume (buy aggressor)
  - Red = bid-side volume (sell aggressor)
  - Gray = auction volume (opening/closing cross, no side info)
- Hover over any candle or volume bar to see exact O/H/L/C, total volume, and bid/offer ratio

### Run both charts simultaneously

```bash
python -m itch.itch_runner \
  --date 01302019 \
  --chart AAPL TSLA \
  --candle-chart AAPL TSLA \
  --candle-interval 60
```

Open http://localhost:8765 (trade chart) and http://localhost:8766 (candle chart) side by side.

### Use a custom file path

```bash
python -m itch.itch_runner --date 01302019 --file /path/to/my/file.NASDAQ_ITCH50 --chart AAPL
```

### Limit messages processed (useful for quick testing)

```bash
# Process only the first 5 million messages
python -m itch.itch_runner --date 01302019 --print-trades AAPL --max-msgs 5000000
```

---

## Running Tests

```bash
pdm run pytest
# or with venv activated:
pytest
```

---

## Project Structure

```
src/
  book/         — Order book, trade model, listener interfaces
  itch/         — ITCH 5.0 parser, feed handler, CLI runners, printers
  analytics/    — VWAP and TWAP calculators
  publishers/   — Kafka trade and VWAP publishers
  web/          — WebSocket chart server, trade and candle chart listeners, HTML charts
  util/         — Shared utilities (time formatting etc.)
tests/          — pytest test suite
data/           — ITCH binary files (gitignored, you provide these)
```

---

## Optional: Kafka

If you have a Kafka broker running, you can publish trades and VWAP events:

```bash
python -m itch.itch_runner --date 01302019 --kafka localhost:9092
```

To replay those events to the trade chart from Kafka (instead of directly from the ITCH file):

```bash
python -m itch.chart_runner --date 01302019 --kafka localhost:9092 --stocks AAPL TSLA
```
