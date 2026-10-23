from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
import re


class ExchangeSegment(IntEnum):
    """Exchange segments supported by DhanHQ."""
    IDX = 0        # Indices (NIFTY, BANKNIFTY, etc.)
    NSE = 1        # National Stock Exchange - Cash / Equity
    NSE_FNO = 2    # NSE Futures & Options
    NSE_CURR = 3   # NSE Currency Derivatives
    BSE = 4        # Bombay Stock Exchange - Cash / Equity
    MCX = 5        # Multi Commodity Exchange
    BSE_CURR = 7   # BSE Currency Derivatives
    BSE_FNO = 8    # BSE Futures & Options

    @classmethod
    def from_str(cls, name: str) -> ExchangeSegment:
        name_upper = name.strip().upper()
        mapping = {
            "IDX": cls.IDX,
            "INDEX": cls.IDX,
            "IDX_I": cls.IDX,
            "NSE": cls.NSE,
            "NSE_EQ": cls.NSE,
            "NSE_FNO": cls.NSE_FNO,
            "NFO": cls.NSE_FNO,
            "NSE_CURR": cls.NSE_CURR,
            "BSE": cls.BSE,
            "BSE_EQ": cls.BSE,
            "MCX": cls.MCX,
            "BSE_CURR": cls.BSE_CURR,
            "BSE_FNO": cls.BSE_FNO,
            "BFO": cls.BSE_FNO,
        }
        if name_upper in mapping:
            return mapping[name_upper]
        raise ValueError(f"Unknown exchange segment: '{name}'")


class FeedMode(IntEnum):
    """Feed modes supported by DhanHQ WebSocket (MarketFeed v2)."""
    TICKER = 15    # LTP and Last Traded Time (LTT)
    QUOTE = 17     # LTP, LTT, Volume, Day Open/High/Low/Close
    FULL = 21      # Full packet with Depth and Open Interest (OI)


class TimeFrame:
    """Timeframe helper for bar intervals."""
    SECOND_1 = "1s"
    SECOND_5 = "5s"
    SECOND_15 = "15s"
    SECOND_30 = "30s"
    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    DAY_1 = "1d"

    @staticmethod
    def to_seconds(tf: str) -> int:
        """Parse human-readable timeframe string into seconds.

        Examples:
            '1s' -> 1
            '5s' -> 5
            '1m' -> 60
            '5m' -> 300
            '15m' -> 900
            '1h' -> 3600
        """
        tf = tf.strip().lower()
        match = re.match(r"^(\d+)([smhd])$", tf)
        if not match:
            raise ValueError(f"Invalid timeframe format '{tf}'. Examples: '5s', '1m', '5m', '1h'")
        val, unit = match.groups()
        multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        return int(val) * multiplier


@dataclass
class Tick:
    """Represents an incoming market tick (LTP / Quote / Depth)."""
    exchange_segment: int
    security_id: str
    symbol: str
    ltp: float
    timestamp: datetime
    volume: int = 0
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    avg_price: float | None = None
    oi: int | None = None
    raw: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        time_str = self.timestamp.strftime("%H:%M:%S")
        return f"<Tick {self.symbol} ({self.security_id}) LTP={self.ltp:.2f} time={time_str}>"


@dataclass
class Bar:
    """Represents an aggregated OHLCV candlestick bar."""
    symbol: str
    exchange_segment: int
    security_id: str
    timeframe: str
    start_time: datetime
    end_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    ticks_count: int = 0
    is_closed: bool = False

    def update(self, price: float, volume: int = 0) -> None:
        """Update bar with a new tick price."""
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += volume
        self.ticks_count += 1

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "security_id": self.security_id,
            "exchange_segment": self.exchange_segment,
            "timeframe": self.timeframe,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "ticks_count": self.ticks_count,
            "is_closed": self.is_closed,
        }

    def __repr__(self) -> str:
        status = "CLOSED" if self.is_closed else "OPEN"
        start_str = self.start_time.strftime("%H:%M:%S")
        return (
            f"<Bar {self.symbol} [{self.timeframe} {start_str}] "
            f"O={self.open:.2f} H={self.high:.2f} L={self.low:.2f} C={self.close:.2f} "
            f"Vol={self.volume} Ticks={self.ticks_count} {status}>"
        )
