from __future__ import annotations

import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Callable, DefaultDict, Deque

from broker.models import Bar, Tick, TimeFrame


class BarAggregator:
    """Aggregates real-time ticks into OHLCV bars across arbitrary timeframes.

    Supports:
        - Arbitrary intervals: '1s', '5s', '15s', '1m', '3m', '5m', '15m', etc.
        - Accurate time-boundary alignment (e.g., minute bars align to :00).
        - Callbacks for bar close (`on_bar_close`) and bar update (`on_bar_update`).
        - In-memory rolling history for each instrument and timeframe.
        - Thread-safe updates and queries.
    """

    def __init__(
        self,
        timeframes: list[str] | None = None,
        max_history: int = 1000,
        on_bar_close: Callable[[Bar], None] | None = None,
        on_bar_update: Callable[[Bar], None] | None = None,
    ):
        self.timeframes = timeframes or [TimeFrame.MINUTE_1]
        self.max_history = max_history
        self._on_bar_close_callbacks: list[Callable[[Bar], None]] = []
        self._on_bar_update_callbacks: list[Callable[[Bar], None]] = []

        if on_bar_close:
            self._on_bar_close_callbacks.append(on_bar_close)
        if on_bar_update:
            self._on_bar_update_callbacks.append(on_bar_update)

        self._lock = threading.Lock()
        # Key: (symbol, timeframe) -> current Bar
        self._current_bars: dict[tuple[str, str], Bar] = {}
        # Key: (symbol, timeframe) -> deque of closed Bars
        self._history: DefaultDict[tuple[str, str], Deque[Bar]] = defaultdict(
            lambda: deque(maxlen=self.max_history)
        )

    def register_on_bar_close(self, callback: Callable[[Bar], None]) -> None:
        """Register a callback invoked when a bar timeframe completes and closes."""
        self._on_bar_close_callbacks.append(callback)

    def register_on_bar_update(self, callback: Callable[[Bar], None]) -> None:
        """Register a callback invoked on each tick that updates an open bar."""
        self._on_bar_update_callbacks.append(callback)

    def add_timeframe(self, timeframe: str) -> None:
        """Add a timeframe to track."""
        if timeframe not in self.timeframes:
            # Validate timeframe
            TimeFrame.to_seconds(timeframe)
            self.timeframes.append(timeframe)

    def process_tick(self, tick: Tick) -> list[Bar]:
        """Process an incoming tick across all tracked timeframes.

        Returns a list of bars that were closed as a result of this tick.
        """
        closed_bars: list[Bar] = []
        updated_bars: list[Bar] = []

        with self._lock:
            for tf in self.timeframes:
                tf_seconds = TimeFrame.to_seconds(tf)
                closed_bar, updated_bar = self._update_timeframe(tick, tf, tf_seconds)
                if closed_bar:
                    closed_bars.append(closed_bar)
                if updated_bar:
                    updated_bars.append(updated_bar)

        # Fire callbacks outside the lock to avoid deadlocks
        for bar in closed_bars:
            for cb in self._on_bar_close_callbacks:
                try:
                    cb(bar)
                except Exception as e:
                    print(f"[BarAggregator] Error in on_bar_close callback: {e}")

        for bar in updated_bars:
            for cb in self._on_bar_update_callbacks:
                try:
                    cb(bar)
                except Exception as e:
                    print(f"[BarAggregator] Error in on_bar_update callback: {e}")

        return closed_bars

    def _update_timeframe(self, tick: Tick, tf: str, tf_seconds: int) -> tuple[Bar | None, Bar | None]:
        key = (tick.symbol, tf)
        current = self._current_bars.get(key)

        ts = tick.timestamp
        # Calculate aligned start and end boundaries
        epoch = ts.timestamp()
        bar_start_epoch = (int(epoch) // tf_seconds) * tf_seconds
        bar_end_epoch = bar_start_epoch + tf_seconds

        if ts.tzinfo is not None:
            start_time = datetime.fromtimestamp(bar_start_epoch, tz=ts.tzinfo)
            end_time = datetime.fromtimestamp(bar_end_epoch, tz=ts.tzinfo)
        else:
            start_time = datetime.fromtimestamp(bar_start_epoch)
            end_time = datetime.fromtimestamp(bar_end_epoch)

        closed_bar: Bar | None = None
        updated_bar: Bar | None = None

        if current is None:
            # First tick for this instrument and timeframe
            current = Bar(
                symbol=tick.symbol,
                exchange_segment=tick.exchange_segment,
                security_id=tick.security_id,
                timeframe=tf,
                start_time=start_time,
                end_time=end_time,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                volume=tick.volume or 0,
                ticks_count=1,
                is_closed=False,
            )
            self._current_bars[key] = current
            updated_bar = current
        elif ts >= current.end_time:
            # Current bar has expired, close it
            current.is_closed = True
            closed_bar = current
            self._history[key].append(current)

            # Start new bar
            current = Bar(
                symbol=tick.symbol,
                exchange_segment=tick.exchange_segment,
                security_id=tick.security_id,
                timeframe=tf,
                start_time=start_time,
                end_time=end_time,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                volume=tick.volume or 0,
                ticks_count=1,
                is_closed=False,
            )
            self._current_bars[key] = current
            updated_bar = current
        else:
            # Update current in-progress bar
            current.update(tick.ltp, tick.volume or 0)
            updated_bar = current

        return closed_bar, updated_bar

    def get_latest_bar(self, symbol: str, timeframe: str = TimeFrame.MINUTE_1) -> Bar | None:
        """Get the current in-progress bar for a symbol and timeframe."""
        with self._lock:
            return self._current_bars.get((symbol, timeframe))

    def get_bars(self, symbol: str, timeframe: str = TimeFrame.MINUTE_1, limit: int = 100) -> list[Bar]:
        """Get the most recent closed bars for a symbol and timeframe."""
        with self._lock:
            history = self._history.get((symbol, timeframe))
            if not history:
                return []
            bars = list(history)
            return bars[-limit:]
