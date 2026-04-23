import datetime
from dataclasses import dataclass

# Auction cross event types (from ITCH 'Q' Cross Trade message)
EVENT_OPEN_CROSS = 'OPEN_CROSS'          # Opening auction (cross_type='O')
EVENT_CLOSE_CROSS = 'CLOSE_CROSS'        # Closing auction (cross_type='C')
EVENT_IPO_CROSS = 'IPO_CROSS'            # IPO / halt auction (cross_type='H')
EVENT_INTRADAY_CROSS = 'INTRADAY_CROSS'  # Intraday cross (cross_type='I')

# Per-stock halt / resume event types (from ITCH 'H' Stock Trading Action message)
EVENT_HALT = 'HALT'          # trading_state='H'
EVENT_PAUSE = 'PAUSE'        # trading_state='P' — LULD / circuit-breaker pause
EVENT_QUOTATION = 'QUOTATION'  # trading_state='Q' — quote-only period
EVENT_RESUME = 'RESUME'      # trading_state='T' — trading resumed


@dataclass
class MarketEvent:
    timestamp_ns: int          # Nanoseconds since midnight
    stock: str                 # Symbol
    event_type: str            # One of the EVENT_* constants above
    trade_date: datetime.date
    price: float = float('nan')  # Auction cross price; NaN → NULL in DB for halt/resume events
    shares: int = 0              # Auction volume; 0 for halt/resume events
    reason: str = ''             # 4-char halt reason code (e.g. 'LUDP', 'MWCB')
