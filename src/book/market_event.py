import datetime
from dataclasses import dataclass

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
    reason: str = ''           # 4-char halt reason code (e.g. 'LUDP', 'MWCB')
