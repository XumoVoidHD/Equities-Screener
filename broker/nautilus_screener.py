from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.config import ActorConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

from broker.conditions import (
    ConditionEvaluationResult,
    ScreenerConditionsConfig,
    ScreenerResult,
    StockMarketState,
)

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
    require_conditions_pass: bool = False


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
    bar: Bar | None = None
    condition_result: ScreenerResult | None = None
    _score: float | None = None

    @property
    def score(self) -> float:
        """Composite urgency score based on conditions passed, volume ratio and price momentum."""
        if self._score is not None:
            return self._score
        base = (self.volume_ratio * 1.5) + abs(self.change_pct)
        if self.condition_result is not None:
            base += self.condition_result.score + (len(self.condition_result.matched_conditions) * 2.0)
        return round(base, 2)

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
            "score": self.score,
            "timestamp": self.timestamp.isoformat(),
            "conditions_passed": self.condition_result.passed if self.condition_result else None,
            "matched_conditions": self.condition_result.matched_conditions if self.condition_result else [],
        }

    def __repr__(self) -> str:
        reasons_str = ", ".join(self.reasons)
        return (
            f"<ScreenerMatch {self.symbol} Price=Rs.{self.price:.2f} "
            f"Chg={self.change_pct:+.2f}% VolRatio={self.volume_ratio:.1f}x Score={self.score:.1f} [{reasons_str}]>"
        )


class NautilusEquityScreenerActor(Actor):
    """High-performance NautilusTrader Actor for real-time equity screening.

    Can be deployed:
      1. Directly inside a NautilusTrader `TradingNode` (listening on the message bus).
      2. Connected to `DhanBroker` streaming any subscribed stock universe (100 to 2,800+ stocks).

    Screening Capabilities:
      - **Chartink-Style Configurable Conditions**:
          - Condition 1: Hourly Close > Daily Open (configurable factor/offset)
          - Condition 2: Hourly Close > ([-1] Daily Close * 0.95)
          - Condition 3: Hourly Close >= [-10] Daily Close
          - Condition 4: 1day RSI(14) > ([-1] 1day RSI(14) + 10)
          - Condition 5: Daily Volume > 5,000,000
      - **Live Intraday Technicals**:
          - Volume Anomaly / Surge against rolling average
          - Single-bar Momentum / Price Spikes
          - Donchian N-bar High/Low Breakouts
          - Bar Range Expansion
      - **Dynamic Leaderboard & Ranked Watchlist**
      - **Nautilus Message Bus Signal Publishing (`SCREENER_ALERT`)**
    """

    def __init__(
        self,
        config: ScreenerConfig | None = None,
        conditions_config: ScreenerConditionsConfig | None = None,
        conditions_json_path: str | Path | None = None,
    ):
        super().__init__(config or ScreenerConfig())
        self._cfg: ScreenerConfig = self.config

        # 1. Configurable Conditions Engine
        if conditions_config is not None:
            self.conditions_config = conditions_config
        elif conditions_json_path is not None and Path(conditions_json_path).exists():
            self.conditions_config = ScreenerConditionsConfig.load_json(conditions_json_path)
        elif Path("config/screener_conditions.json").exists():
            self.conditions_config = ScreenerConditionsConfig.load_json("config/screener_conditions.json")
        else:
            self.conditions_config = ScreenerConditionsConfig()

        # 2. Multi-Timeframe Market States (symbol -> StockMarketState)
        self._market_states: dict[str, StockMarketState] = {}

        # 3. Rolling bar histories for intraday technicals
        self._volumes: dict[str, deque] = {}
        self._highs: dict[str, deque] = {}
        self._lows: dict[str, deque] = {}
        self._closes: dict[str, deque] = {}

        # 4. Watchlist & Matches
        self._matches: dict[str, ScreenerMatch] = {}
        self._match_callbacks: list[Callable[[ScreenerMatch], None]] = []

        # 5. Metrics
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
        logger.info(
            "NautilusEquityScreenerActor started (conditions=%s, lookback=%d bars)",
            self.conditions_config.screener_name,
            self._cfg.lookback_bars,
        )

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

    # -------------------------------------------------------------------------
    # Context Management & Preloading
    # -------------------------------------------------------------------------

    def preload_daily_context(
        self,
        symbol: str,
        daily_open: float,
        daily_closes: list[float],
        daily_volumes: list[float] | None = None,
        daily_highs: list[float] | None = None,
        daily_lows: list[float] | None = None,
        daily_volume: float = 0.0,
    ) -> StockMarketState:
        """Preload historical daily context for a symbol to enable instant condition evaluation."""
        clean_sym = symbol.strip().upper()
        hourly_px = daily_closes[-1] if daily_closes else daily_open

        state = StockMarketState(
            symbol=clean_sym,
            hourly_close=hourly_px,
            daily_open=daily_open,
            daily_closes=list(daily_closes),
            daily_volumes=list(daily_volumes or []),
            daily_highs=list(daily_highs or []),
            daily_lows=list(daily_lows or []),
            daily_volume=daily_volume,
        )
        self._market_states[clean_sym] = state
        return state

    def fetch_and_preload_daily_context(
        self,
        broker: Any,
        symbols: list[str],
        days: int = 40,
    ) -> dict[str, bool]:
        """Fetch historical daily candles using DhanBroker REST API and preload context.

        Args:
            broker: DhanBroker instance
            symbols: List of equity symbols (e.g. ['RELIANCE', 'TCS'])
            days: Lookback period in days (default 40 to satisfy RSI(14) and 10-day lookback)
        """
        results = {}
        to_dt = datetime.now()
        from_dt = to_dt - timedelta(days=days)
        from_str = from_dt.strftime("%Y-%m-%d")
        to_str = to_dt.strftime("%Y-%m-%d")

        for sym in symbols:
            clean_sym = sym.strip().upper()
            try:
                hist_data = broker.get_historical_daily_bars(clean_sym, from_str, to_str)
                candles = hist_data.get("data", {}) if isinstance(hist_data, dict) else {}
                closes = candles.get("close", [])
                opens = candles.get("open", [])
                vols = candles.get("volume", [])
                highs = candles.get("high", [])
                lows = candles.get("low", [])

                if closes:
                    d_open = opens[-1] if opens else closes[-1]
                    d_vol = vols[-1] if vols else 0.0
                    self.preload_daily_context(
                        symbol=clean_sym,
                        daily_open=d_open,
                        daily_closes=closes,
                        daily_volumes=vols,
                        daily_highs=highs,
                        daily_lows=lows,
                        daily_volume=d_vol,
                    )
                    results[clean_sym] = True
                    logger.info("Preloaded %d daily bars for %s", len(closes), clean_sym)
                else:
                    results[clean_sym] = False
                    logger.warning("No daily history returned for %s", clean_sym)
            except Exception as e:
                logger.error("Failed to preload daily history for %s: %s", clean_sym, e)
                results[clean_sym] = False

        return results

    def get_or_create_market_state(
        self,
        symbol: str,
        close_px: float,
        open_px: float,
        volume: float,
    ) -> StockMarketState:
        """Get or initialize the StockMarketState for a symbol."""
        clean_sym = symbol.strip().upper()
        if clean_sym not in self._market_states:
            self._market_states[clean_sym] = StockMarketState(
                symbol=clean_sym,
                hourly_close=close_px,
                daily_open=open_px,
                daily_closes=[close_px],
                daily_volume=volume,
            )
        else:
            state = self._market_states[clean_sym]
            state.hourly_close = close_px
            if state.daily_open == 0.0:
                state.daily_open = open_px
            state.daily_volume += volume
            # Invalidate RSI cache when new price arrives
            state._rsi_cache.clear()
        return self._market_states[clean_sym]

    # -------------------------------------------------------------------------
    # Bar Processing & Screening Pipeline
    # -------------------------------------------------------------------------

    def on_bar(self, bar: Bar) -> None:
        """Nautilus standard event handler called for every incoming bar."""
        self.total_bars_processed += 1

        inst_id = bar.bar_type.instrument_id
        symbol = inst_id.symbol.value
        venue = inst_id.venue.value

        open_px = bar.open.as_double()
        high_px = bar.high.as_double()
        low_px = bar.low.as_double()
        close_px = bar.close.as_double()
        vol = bar.volume.as_double()

        lookback = self._cfg.lookback_bars

        # Update or retrieve Multi-Timeframe State
        state = self.get_or_create_market_state(symbol, close_px, open_px, vol)

        # 1. Evaluate Chartink Configurable Conditions
        condition_result = self.conditions_config.evaluate(state)
        conditions_passed = condition_result.passed

        # 2. Evaluate Bar-level Intraday Dynamics
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

        if len(vol_hist) >= 2:
            avg_vol = sum(vol_hist) / len(vol_hist) if vol_hist else 0
            if avg_vol > 0:
                vol_ratio = vol / avg_vol
                if self._cfg.min_volume_ratio > 0 and vol_ratio >= self._cfg.min_volume_ratio:
                    reasons.append(f"VOLUME_SURGE ({vol_ratio:.1f}x)")

        if open_px > 0:
            change_pct = ((close_px - open_px) / open_px) * 100.0
            if self._cfg.min_price_change_pct > 0 and abs(change_pct) >= self._cfg.min_price_change_pct:
                direction = "SPIKE" if change_pct > 0 else "DROP"
                reasons.append(f"PRICE_{direction} ({change_pct:+.2f}%)")
        else:
            change_pct = 0.0

        if open_px > 0:
            range_pct = ((high_px - low_px) / open_px) * 100.0
            if self._cfg.min_bar_range_pct > 0 and range_pct >= self._cfg.min_bar_range_pct:
                reasons.append(f"RANGE_EXPANSION ({range_pct:.2f}%)")
        else:
            range_pct = 0.0

        if self._cfg.check_breakout and len(high_hist) >= 5:
            max_prev_high = max(high_hist)
            min_prev_low = min(low_hist)
            if high_px > max_prev_high:
                reasons.append(f"NEW_{len(high_hist)}_BAR_HIGH")
            elif low_px < min_prev_low:
                reasons.append(f"NEW_{len(low_hist)}_BAR_LOW")

        # Update historical windows
        vol_hist.append(vol)
        high_hist.append(high_px)
        low_hist.append(low_px)
        close_hist.append(close_px)

        # 3. Determine Overall Screener Match Trigger
        trigger_match = False
        if conditions_passed:
            trigger_match = True
            for m_name in condition_result.matched_conditions:
                reasons.append(f"CONDITION: {m_name}")
        elif not self._cfg.require_conditions_pass and reasons:
            trigger_match = True

        if trigger_match:
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
                condition_result=condition_result,
            )

            self._matches[symbol] = match

            if self._cfg.publish_signals and self.is_initialized:
                try:
                    self.publish_signal(
                        name="SCREENER_ALERT",
                        value=float(close_px),
                        ts_event=bar.ts_event,
                    )
                except Exception as e:
                    logger.debug("Could not publish Nautilus signal: %s", e)

            for cb in self._match_callbacks:
                try:
                    cb(match)
                except Exception as e:
                    logger.error("Error in screener match callback: %s", e)

    def evaluate_symbol(self, symbol: str) -> ScreenerResult:
        """Manually trigger condition evaluation on a symbol's current state."""
        clean_sym = symbol.strip().upper()
        if clean_sym not in self._market_states:
            raise KeyError(f"No state preloaded for symbol '{clean_sym}'")
        return self.conditions_config.evaluate(self._market_states[clean_sym])

    def get_watchlist(self, top_n: int = 20) -> list[ScreenerMatch]:
        """Get the current screener watchlist sorted by urgency score."""
        sorted_matches = sorted(
            self._matches.values(),
            key=lambda m: m.score,
            reverse=True,
        )
        return sorted_matches[:top_n]

    # -------------------------------------------------------------------------
    # Dynamic Condition Configuration API
    # -------------------------------------------------------------------------

    def enable_condition(self, condition_id: str, enabled: bool = True) -> None:
        """Enable or disable any condition rule."""
        self.conditions_config.enable_condition(condition_id, enabled)

    def set_condition_param(self, condition_id: str, param_name: str, value: Any) -> None:
        """Set a parameter value on any condition rule."""
        self.conditions_config.set_param(condition_id, param_name, value)

    def load_conditions(self, path: str | Path) -> None:
        """Load condition configuration from a JSON file."""
        self.conditions_config = ScreenerConditionsConfig.load_json(path)

    def save_conditions(self, path: str | Path) -> None:
        """Save condition configuration to a JSON file."""
        self.conditions_config.save_json(path)

    # -------------------------------------------------------------------------
    # Dhan Broker Direct Integration Bridge
    # -------------------------------------------------------------------------

    def on_dhan_bar(self, broker_bar: Any) -> None:
        """Convert a broker.models.Bar to a Nautilus Bar and process it."""
        try:
            venue_str = "NSE" if getattr(broker_bar, "exchange_segment", 1) == 1 else "BSE"
            inst_id_str = f"{broker_bar.symbol}.{venue_str}"

            tf_str = getattr(broker_bar, "timeframe", "1m").upper()
            if tf_str.endswith("S"):
                spec_str = f"{tf_str[:-1]}-SECOND-LAST-EXTERNAL"
            elif tf_str.endswith("M"):
                spec_str = f"{tf_str[:-1]}-MINUTE-LAST-EXTERNAL"
            elif tf_str.endswith("H"):
                spec_str = f"{tf_str[:-1]}-HOUR-LAST-EXTERNAL"
            else:
                spec_str = "1-MINUTE-LAST-EXTERNAL"

            bar_type = BarType.from_str(f"{inst_id_str}-{spec_str}")
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


