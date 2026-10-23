from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.config import ActorConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

logger = logging.getLogger("dhan.nautilus.screener")


class ScreenerConfig(ActorConfig):
    """Configuration for NautilusEquityScreenerActor."""

    lookback_bars: int = 10
    min_volume_ratio: float = 1.5
    min_price_change_pct: float = 0.5
    check_breakout: bool = True
    min_bar_range_pct: float = 0.3
    watchlist_size: int = 50
    publish_signals: bool = True



@dataclass
class ScreenerMatch:
    """Represents an instrument match triggered by the Nautilus Screener Actor."""

    symbol: str
    instrument_id: str
    venue: str
    price: float
    volume: float
    volume_ratio: float
    change_pct: float
    range_pct: float
    reasons: list[str]
    timestamp: datetime
    bar: Bar

    @property
    def score(self) -> float:
        """Composite urgency score based on volume ratio and price momentum."""
        return (self.volume_ratio * 1.5) + abs(self.change_pct)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "price": self.price,
            "volume": self.volume,
            "volume_ratio": round(self.volume_ratio, 2),
            "change_pct": round(self.change_pct, 2),
            "range_pct": round(self.range_pct, 2),
            "reasons": self.reasons,
            "score": round(self.score, 2),
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        reasons_str = ", ".join(self.reasons)
        return (
            f"<ScreenerMatch {self.symbol} Price=Rs.{self.price:.2f} "
            f"Chg={self.change_pct:+.2f}% VolRatio={self.volume_ratio:.1f}x [{reasons_str}]>"
        )


class NautilusEquityScreenerActor(Actor):
    """High-performance NautilusTrader Actor for real-time equity screening.

    Can be deployed:
      1. Directly inside a NautilusTrader `TradingNode` (listening on the message bus).
      2. Connected to `DhanBroker` streaming any subscribed stock universe (100 to 2,800+ stocks).

    Screening Capabilities:
      - **Volume Anomaly / Surge**: Compares incoming bar volume against rolling average.
      - **Momentum / Price Spike**: Detects sharp single-bar percentage changes.
      - **Donchian Breakout**: Detects new rolling N-bar highs or lows.
      - **Range Expansion**: Identifies expanded volatility bars (High - Low).
      - **Dynamic Leaderboard**: Maintains a real-time ranked watchlist of top momentum candidates.
      - **Nautilus Signal Dispatch**: Emits `publish_signal('SCREENER_ALERT', ...)` on message bus.
    """

    def __init__(self, config: ScreenerConfig | None = None):
        super().__init__(config or ScreenerConfig())
        self._cfg: ScreenerConfig = self.config

        # Per-symbol rolling histories
        self._volumes: dict[str, deque] = {}
        self._highs: dict[str, deque] = {}
        self._lows: dict[str, deque] = {}
        self._closes: dict[str, deque] = {}

        # Watchlist & Matches
        self._matches: dict[str, ScreenerMatch] = {}
        self._match_callbacks: list[Callable[[ScreenerMatch], None]] = []

        # Metrics
        self.total_bars_processed = 0
        self.total_matches_found = 0

    def on_start(self) -> None:
        """Invoked when Actor starts."""
        self._volumes.clear()
        self._highs.clear()
        self._lows.clear()
        self._closes.clear()
        self._matches.clear()
        self.total_bars_processed = 0
        self.total_matches_found = 0
        logger.info("NautilusEquityScreenerActor started (lookback=%d bars)", self._cfg.lookback_bars)

    def on_stop(self) -> None:
        """Invoked when Actor stops."""
        logger.info(
            "NautilusEquityScreenerActor stopped. Processed %d bars, triggered %d matches.",
            self.total_bars_processed,
            self.total_matches_found,
        )

    def on_match(self, callback: Callable[[ScreenerMatch], None]) -> None:
        """Register a callback called whenever an instrument triggers screening rules."""
        self._match_callbacks.append(callback)

    def on_bar(self, bar: Bar) -> None:
        """Nautilus standard event handler called for every incoming bar."""
        self.total_bars_processed += 1

        # Extract instrument identifiers and OHLCV
        inst_id = bar.bar_type.instrument_id
        symbol = inst_id.symbol.value
        venue = inst_id.venue.value

        open_px = bar.open.as_double()
        high_px = bar.high.as_double()
        low_px = bar.low.as_double()
        close_px = bar.close.as_double()
        vol = bar.volume.as_double()

        lookback = self._cfg.lookback_bars

        # Initialize rolling deques for symbol
        if symbol not in self._volumes:
            self._volumes[symbol] = deque(maxlen=lookback)
            self._highs[symbol] = deque(maxlen=lookback)
            self._lows[symbol] = deque(maxlen=lookback)
            self._closes[symbol] = deque(maxlen=lookback)

        vol_hist = self._volumes[symbol]
        high_hist = self._highs[symbol]
        low_hist = self._lows[symbol]
        close_hist = self._closes[symbol]

        reasons = []
        vol_ratio = 1.0

        # Evaluate rules once we have minimum baseline history (or even on 2+ bars)
        if len(vol_hist) >= 2:
            avg_vol = sum(vol_hist) / len(vol_hist) if vol_hist else 0
            if avg_vol > 0:
                vol_ratio = vol / avg_vol
                if self._cfg.min_volume_ratio > 0 and vol_ratio >= self._cfg.min_volume_ratio:
                    reasons.append(f"VOLUME_SURGE ({vol_ratio:.1f}x)")

        # Calculate Price Change %
        if open_px > 0:
            change_pct = ((close_px - open_px) / open_px) * 100.0
            if self._cfg.min_price_change_pct > 0 and abs(change_pct) >= self._cfg.min_price_change_pct:
                direction = "SPIKE" if change_pct > 0 else "DROP"
                reasons.append(f"PRICE_{direction} ({change_pct:+.2f}%)")
        else:
            change_pct = 0.0

        # Calculate Bar Range % (High - Low)
        if open_px > 0:
            range_pct = ((high_px - low_px) / open_px) * 100.0
            if self._cfg.min_bar_range_pct > 0 and range_pct >= self._cfg.min_bar_range_pct:
                reasons.append(f"RANGE_EXPANSION ({range_pct:.2f}%)")
        else:
            range_pct = 0.0

        # Donchian Breakout check
        if self._cfg.check_breakout and len(high_hist) >= 5:
            max_prev_high = max(high_hist)
            min_prev_low = min(low_hist)
            if high_px > max_prev_high:
                reasons.append(f"NEW_{len(high_hist)}_BAR_HIGH")
            elif low_px < min_prev_low:
                reasons.append(f"NEW_{len(low_hist)}_BAR_LOW")

        # Update historical windows with current bar values
        vol_hist.append(vol)
        high_hist.append(high_px)
        low_hist.append(low_px)
        close_hist.append(close_px)

        # Trigger Screener Match if any criteria matched
        if reasons:
            self.total_matches_found += 1
            ts = datetime.fromtimestamp(bar.ts_event / 1e9, tz=timezone.utc)
            match = ScreenerMatch(
                symbol=symbol,
                instrument_id=str(inst_id),
                venue=venue,
                price=close_px,
                volume=vol,
                volume_ratio=vol_ratio,
                change_pct=change_pct,
                range_pct=range_pct,
                reasons=reasons,
                timestamp=ts,
                bar=bar,
            )

            # Update active watchlist
            self._matches[symbol] = match

            # Publish Nautilus Signal if registered on message bus
            if self._cfg.publish_signals and self.is_initialized:
                try:
                    self.publish_signal(
                        name="SCREENER_ALERT",
                        value=float(close_px),
                        ts_event=bar.ts_event,
                    )
                except Exception as e:
                    logger.debug("Could not publish Nautilus signal: %s", e)

            # Invoke user callbacks
            for cb in self._match_callbacks:
                try:
                    cb(match)
                except Exception as e:
                    logger.error("Error in screener match callback: %s", e)

    def get_watchlist(self, top_n: int = 20) -> list[ScreenerMatch]:
        """Get the current screener watchlist sorted by urgency score."""
        sorted_matches = sorted(
            self._matches.values(),
            key=lambda m: m.score,
            reverse=True,
        )
        return sorted_matches[:top_n]

    # -------------------------------------------------------------------------
    # Dhan Broker Direct Integration Bridge
    # -------------------------------------------------------------------------

    def on_dhan_bar(self, broker_bar: Any) -> None:
        """Convert a broker.models.Bar to a Nautilus Bar and process it."""
        try:
            # Build Nautilus InstrumentId (e.g. 'RELIANCE.NSE' or 'ABB.BSE')
            venue_str = "NSE" if getattr(broker_bar, "exchange_segment", 1) == 1 else "BSE"
            inst_id_str = f"{broker_bar.symbol}.{venue_str}"

            # Format BarType (e.g. 'RELIANCE.NSE-1-MINUTE-LAST-EXTERNAL')
            tf_str = getattr(broker_bar, "timeframe", "1m").upper()
            if tf_str.endswith("S"):
                spec_str = f"{tf_str[:-1]}-SECOND-LAST-EXTERNAL"
            elif tf_str.endswith("M"):
                spec_str = f"{tf_str[:-1]}-MINUTE-LAST-EXTERNAL"
            else:
                spec_str = "1-MINUTE-LAST-EXTERNAL"

            bar_type = BarType.from_str(f"{inst_id_str}-{spec_str}")

            # Unix nanosecond timestamp
            ts_dt = getattr(broker_bar, "end_time", datetime.now())
            ts_ns = int(ts_dt.timestamp() * 1e9)

            nautilus_bar = Bar(
                bar_type=bar_type,
                open=Price(float(broker_bar.open), 2),
                high=Price(float(broker_bar.high), 2),
                low=Price(float(broker_bar.low), 2),
                close=Price(float(broker_bar.close), 2),
                volume=Quantity(float(getattr(broker_bar, "volume", 0) or 0), 0),
                ts_event=ts_ns,
                ts_init=ts_ns,
            )

            # Pass directly into Nautilus Actor event pipeline
            self.on_bar(nautilus_bar)

        except Exception as e:
            logger.error("Error converting Dhan bar to Nautilus Bar: %s", e)

    def attach_to_broker(self, broker: Any, on_update: bool = False) -> None:
        """Hook this screener actor into DhanBroker.

        Args:
            broker: DhanBroker instance
            on_update: If True, also evaluate screening rules on open bar updates.
        """
        broker.on_bar_close(self.on_dhan_bar)
        if on_update:
            broker.on_bar_update(self.on_dhan_bar)
        self.on_start()
        logger.info(
            "NautilusEquityScreenerActor successfully attached to DhanBroker (on_update=%s).",
            on_update,
        )

