from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np

logger = logging.getLogger("dhan.screener.conditions")


def compute_wilder_rsi(prices: list[float] | np.ndarray, window: int = 14) -> np.ndarray:
    """Compute standard Wilder's Relative Strength Index (RSI).

    Matches standard technical analysis platforms (e.g. Chartink, TradingView).
    """
    arr = np.asarray(prices, dtype=float)
    n = len(arr)
    if n <= window:
        return np.full(n, 50.0)

    deltas = np.diff(arr)
    seed = deltas[:window]
    up = seed[seed >= 0].sum() / window
    down = -seed[seed < 0].sum() / window
    rs = up / down if down != 0 else 0
    rsi = np.zeros(n)
    rsi[:window] = 100.0 - (100.0 / (1.0 + rs)) if down != 0 else 100.0

    for i in range(window, len(deltas)):
        delta = deltas[i]
        upval = delta if delta > 0 else 0.0
        downval = -delta if delta < 0 else 0.0

        up = (up * (window - 1) + upval) / window
        down = (down * (window - 1) + downval) / window

        rs = up / down if down != 0 else 0.0
        rsi[i + 1] = 100.0 - (100.0 / (1.0 + rs)) if down != 0 else 100.0

    return rsi


@dataclass
class StockMarketState:
    """Snapshot of a stock's real-time and historical multi-timeframe state."""

    symbol: str
    hourly_close: float
    daily_open: float
    daily_closes: list[float] = field(default_factory=list)
    daily_volumes: list[float] = field(default_factory=list)
    daily_highs: list[float] = field(default_factory=list)
    daily_lows: list[float] = field(default_factory=list)
    daily_volume: float = 0.0
    _rsi_cache: dict[int, np.ndarray] = field(default_factory=dict, repr=False)

    @property
    def full_daily_closes(self) -> list[float]:
        """Returns the full daily close timeline ending with today's live hourly_close."""
        if not self.daily_closes:
            return [self.hourly_close]
        if abs(self.daily_closes[-1] - self.hourly_close) < 1e-4:
            return list(self.daily_closes)
        return list(self.daily_closes) + [self.hourly_close]

    @property
    def full_daily_volumes(self) -> list[float]:
        """Returns the full daily volume timeline ending with today's daily_volume."""
        if not self.daily_volumes:
            return [self.daily_volume]
        if self.daily_volume > 0 and abs(self.daily_volumes[-1] - self.daily_volume) > 1e-4:
            return list(self.daily_volumes) + [self.daily_volume]
        return list(self.daily_volumes)

    def get_daily_close(self, lookback_days: int = 0) -> float | None:
        """Get daily close with lookback.

        lookback_days=0 -> today's close (hourly_close).
        lookback_days=1 -> yesterday's close ([-1] Daily Close).
        lookback_days=10 -> close 10 trading sessions ago ([-10] Daily Close).
        """
        closes = self.full_daily_closes
        idx = -1 - lookback_days
        if len(closes) >= abs(idx):
            return closes[idx]
        return None

    def get_daily_volume(self, lookback_days: int = 0) -> float | None:
        """Get daily volume with lookback.

        lookback_days=0 -> today's cumulative daily volume.
        lookback_days=1 -> yesterday's daily volume.
        """
        vols = self.full_daily_volumes
        idx = -1 - lookback_days
        if len(vols) >= abs(idx):
            return vols[idx]
        return None

    def get_daily_rsi(self, window: int = 14, lookback_days: int = 0) -> float | None:
        """Get daily Wilder's RSI(window) with lookback.

        lookback_days=0 -> Today's live/latest RSI(window).
        lookback_days=1 -> Yesterday's daily RSI(window) ([-1] 1day RSI).
        """
        if window not in self._rsi_cache:
            prices = self.full_daily_closes
            if len(prices) < 2:
                prices = [self.daily_open, self.hourly_close]
            self._rsi_cache[window] = compute_wilder_rsi(prices, window=window)

        series = self._rsi_cache[window]
        idx = -1 - lookback_days
        if len(series) >= abs(idx):
            return float(series[idx])
        return None


@dataclass
class ConditionEvaluationResult:
    """Evaluation output for a single condition."""

    condition_id: str
    name: str
    enabled: bool
    passed: bool
    left_value: float | None = None
    right_value: float | None = None
    operator: str = ""
    message: str = ""

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        if not self.enabled:
            return f"<{self.condition_id} DISABLED>"
        return (
            f"<{self.condition_id} {status}: {self.left_value:.2f} {self.operator} "
            f"{self.right_value:.2f} | {self.message}>"
        )


@dataclass
class ScreenerResult:
    """Full evaluation result across all conditions for an instrument."""

    symbol: str
    passed: bool
    matched_conditions: list[str]
    failed_conditions: list[str]
    evaluations: dict[str, ConditionEvaluationResult]
    score: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        status = "MATCH" if self.passed else "NO_MATCH"
        matched_str = ", ".join(self.matched_conditions) if self.matched_conditions else "None"
        return f"<ScreenerResult {self.symbol} [{status}] Matched: [{matched_str}]>"


def _compare(left: float | None, op: str, right: float | None) -> bool:
    """Evaluate comparator operator."""
    if left is None or right is None:
        return False
    if op == ">":
        return left > right
    elif op == ">=":
        return left >= right
    elif op == "<":
        return left < right
    elif op == "<=":
        return left <= right
    elif op == "==":
        return abs(left - right) < 1e-5
    raise ValueError(f"Unsupported operator '{op}'")


# -----------------------------------------------------------------------------
# Configurable Condition Rules
# -----------------------------------------------------------------------------


class Condition1HourlyAboveDailyOpen:
    """Condition 1: Hourly Close > Daily Open (with configurable factor and offset)."""

    def __init__(
        self,
        enabled: bool = True,
        factor: float = 1.0,
        offset: float = 0.0,
        operator: str = ">",
        connector: str = "AND",
        id: str = "condition_1",
        name: str = "Hourly Close > Daily Open",
    ):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.factor = float(factor)
        self.offset = float(offset)
        self.operator = operator
        self.connector = connector

    def evaluate(self, state: StockMarketState) -> ConditionEvaluationResult:
        if not self.enabled:
            return ConditionEvaluationResult(
                condition_id=self.id, name=self.name, enabled=False, passed=True, message="Disabled"
            )

        left = state.hourly_close
        right = (state.daily_open * self.factor) + self.offset
        passed = _compare(left, self.operator, right)
        msg = f"Hourly Close ({left:.2f}) {self.operator} Daily Open ({right:.2f})"
        return ConditionEvaluationResult(
            condition_id=self.id,
            name=self.name,
            enabled=True,
            passed=passed,
            left_value=left,
            right_value=right,
            operator=self.operator,
            message=msg,
        )


class Condition2HourlyAbovePrevCloseDiscount:
    """Condition 2: Hourly Close > ([-1] Daily Close * 0.95)."""

    def __init__(
        self,
        enabled: bool = True,
        lookback_days: int = 1,
        factor: float = 0.95,
        operator: str = ">",
        connector: str = "AND",
        id: str = "condition_2",
        name: str = "Hourly Close > ([-1] Daily Close * 0.95)",
    ):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.lookback_days = int(lookback_days)
        self.factor = float(factor)
        self.operator = operator
        self.connector = connector

    def evaluate(self, state: StockMarketState) -> ConditionEvaluationResult:
        if not self.enabled:
            return ConditionEvaluationResult(
                condition_id=self.id, name=self.name, enabled=False, passed=True, message="Disabled"
            )

        left = state.hourly_close
        prev_close = state.get_daily_close(self.lookback_days)
        if prev_close is None:
            return ConditionEvaluationResult(
                condition_id=self.id,
                name=self.name,
                enabled=True,
                passed=False,
                message=f"Missing historical close for lookback={self.lookback_days}",
            )

        right = prev_close * self.factor
        passed = _compare(left, self.operator, right)
        msg = f"Hourly Close ({left:.2f}) {self.operator} [-{self.lookback_days}] Close * {self.factor} ({right:.2f})"
        return ConditionEvaluationResult(
            condition_id=self.id,
            name=self.name,
            enabled=True,
            passed=passed,
            left_value=left,
            right_value=right,
            operator=self.operator,
            message=msg,
        )


class Condition3HourlyAbovePastClose:
    """Condition 3: Hourly Close >= [-10] Daily Close."""

    def __init__(
        self,
        enabled: bool = True,
        lookback_days: int = 10,
        factor: float = 1.0,
        operator: str = ">=",
        connector: str = "AND",
        id: str = "condition_3",
        name: str = "Hourly Close >= [-10] Daily Close",
    ):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.lookback_days = int(lookback_days)
        self.factor = float(factor)
        self.operator = operator
        self.connector = connector

    def evaluate(self, state: StockMarketState) -> ConditionEvaluationResult:
        if not self.enabled:
            return ConditionEvaluationResult(
                condition_id=self.id, name=self.name, enabled=False, passed=True, message="Disabled"
            )

        left = state.hourly_close
        past_close = state.get_daily_close(self.lookback_days)
        if past_close is None:
            return ConditionEvaluationResult(
                condition_id=self.id,
                name=self.name,
                enabled=True,
                passed=False,
                message=f"Insufficient history: missing [-{self.lookback_days}] Daily Close",
            )

        right = past_close * self.factor
        passed = _compare(left, self.operator, right)
        msg = f"Hourly Close ({left:.2f}) {self.operator} [-{self.lookback_days}] Close ({right:.2f})"
        return ConditionEvaluationResult(
            condition_id=self.id,
            name=self.name,
            enabled=True,
            passed=passed,
            left_value=left,
            right_value=right,
            operator=self.operator,
            message=msg,
        )


class Condition4DailyRSIJump:
    """Condition 4: 1day RSI(14) > ([-1] 1day RSI(14) + 10)."""

    def __init__(
        self,
        enabled: bool = True,
        rsi_window: int = 14,
        lookback_days: int = 1,
        rsi_delta: float = 10.0,
        operator: str = ">",
        connector: str = "AND",
        id: str = "condition_4",
        name: str = "1day RSI(14) > ([-1] 1day RSI(14) + 10)",
    ):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.rsi_window = int(rsi_window)
        self.lookback_days = int(lookback_days)
        self.rsi_delta = float(rsi_delta)
        self.operator = operator
        self.connector = connector

    def evaluate(self, state: StockMarketState) -> ConditionEvaluationResult:
        if not self.enabled:
            return ConditionEvaluationResult(
                condition_id=self.id, name=self.name, enabled=False, passed=True, message="Disabled"
            )

        current_rsi = state.get_daily_rsi(window=self.rsi_window, lookback_days=0)
        prev_rsi = state.get_daily_rsi(window=self.rsi_window, lookback_days=self.lookback_days)

        if current_rsi is None or prev_rsi is None:
            return ConditionEvaluationResult(
                condition_id=self.id,
                name=self.name,
                enabled=True,
                passed=False,
                message=f"Insufficient data to compute {self.rsi_window}-day RSI",
            )

        right = prev_rsi + self.rsi_delta
        passed = _compare(current_rsi, self.operator, right)
        msg = f"Today RSI ({current_rsi:.2f}) {self.operator} Yesterday RSI + {self.rsi_delta:.1f} ({right:.2f})"
        return ConditionEvaluationResult(
            condition_id=self.id,
            name=self.name,
            enabled=True,
            passed=passed,
            left_value=current_rsi,
            right_value=right,
            operator=self.operator,
            message=msg,
        )


class Condition5DailyVolumeThreshold:
    """Condition 5: Daily Volume > 5,000,000 (Disabled by default)."""

    def __init__(
        self,
        enabled: bool = False,
        min_volume: float = 5000000.0,
        lookback_days: int = 0,
        operator: str = ">",
        connector: str = "AND",
        id: str = "condition_5",
        name: str = "Daily Volume > 5,000,000",
    ):
        self.id = id
        self.name = name
        self.enabled = enabled
        self.min_volume = float(min_volume)
        self.lookback_days = int(lookback_days)
        self.operator = operator
        self.connector = connector

    def evaluate(self, state: StockMarketState) -> ConditionEvaluationResult:
        if not self.enabled:
            return ConditionEvaluationResult(
                condition_id=self.id, name=self.name, enabled=False, passed=True, message="Disabled"
            )

        vol = state.get_daily_volume(self.lookback_days)
        if vol is None:
            return ConditionEvaluationResult(
                condition_id=self.id,
                name=self.name,
                enabled=True,
                passed=False,
                message="Missing daily volume",
            )

        passed = _compare(vol, self.operator, self.min_volume)
        msg = f"Daily Volume ({vol:,.0f}) {self.operator} Threshold ({self.min_volume:,.0f})"
        return ConditionEvaluationResult(
            condition_id=self.id,
            name=self.name,
            enabled=True,
            passed=passed,
            left_value=vol,
            right_value=self.min_volume,
            operator=self.operator,
            message=msg,
        )


# -----------------------------------------------------------------------------
# Master Screener Conditions Configuration
# -----------------------------------------------------------------------------


class ScreenerConditionsConfig:
    """Master configuration for the Chartink-style multi-condition screener.

    Supports:
        - Enabling/disabling any condition individually.
        - Customizing threshold parameters (factor, offset, lookback_days, rsi_delta, min_volume).
        - Global combination logic ('AND' vs 'OR').
        - Serialization to and from JSON configuration files.
    """

    def __init__(
        self,
        screener_name: str = "Chartink Momentum & RSI Surge",
        logic: str = "AND",
        condition_1: Condition1HourlyAboveDailyOpen | None = None,
        condition_2: Condition2HourlyAbovePrevCloseDiscount | None = None,
        condition_3: Condition3HourlyAbovePastClose | None = None,
        condition_4: Condition4DailyRSIJump | None = None,
        condition_5: Condition5DailyVolumeThreshold | None = None,
    ):
        self.screener_name = screener_name
        self.logic = logic.upper().strip()
        self.condition_1 = condition_1 or Condition1HourlyAboveDailyOpen()
        self.condition_2 = condition_2 or Condition2HourlyAbovePrevCloseDiscount()
        self.condition_3 = condition_3 or Condition3HourlyAbovePastClose()
        self.condition_4 = condition_4 or Condition4DailyRSIJump()
        self.condition_5 = condition_5 or Condition5DailyVolumeThreshold()

        self._rules = {
            "condition_1": self.condition_1,
            "condition_2": self.condition_2,
            "condition_3": self.condition_3,
            "condition_4": self.condition_4,
            "condition_5": self.condition_5,
        }

    def get_rule(self, condition_id: str) -> Any:
        return self._rules.get(condition_id.lower().strip())

    def enable_condition(self, condition_id: str, enabled: bool = True) -> None:
        """Enable or disable an individual condition."""
        rule = self.get_rule(condition_id)
        if rule:
            rule.enabled = enabled

    def set_param(self, condition_id: str, param_name: str, value: Any) -> None:
        """Dynamically configure any parameter of a condition."""
        rule = self.get_rule(condition_id)
        if rule and hasattr(rule, param_name):
            setattr(rule, param_name, value)
        else:
            raise KeyError(f"Invalid condition_id '{condition_id}' or parameter '{param_name}'")

    def evaluate(self, state: StockMarketState) -> ScreenerResult:
        """Evaluate all conditions on the provided market state."""
        evals: dict[str, ConditionEvaluationResult] = {}
        matched = []
        failed = []

        active_rules = [r for r in self._rules.values() if r.enabled]

        for rule in self._rules.values():
            res = rule.evaluate(state)
            evals[rule.id] = res
            if rule.enabled:
                if res.passed:
                    matched.append(rule.name)
                else:
                    failed.append(rule.name)

        if not active_rules:
            overall_passed = True
        elif self.logic == "OR":
            overall_passed = len(matched) > 0
        else:  # AND logic
            overall_passed = len(failed) == 0

        # Score calculation based on margin of passage
        score = 0.0
        if evals["condition_1"].passed and evals["condition_1"].enabled:
            score += 1.0
        if evals["condition_4"].passed and evals["condition_4"].enabled:
            # Add RSI delta to urgency score
            left = evals["condition_4"].left_value or 0
            right = evals["condition_4"].right_value or 0
            score += max(0.0, left - right)

        return ScreenerResult(
            symbol=state.symbol,
            passed=overall_passed,
            matched_conditions=matched,
            failed_conditions=failed,
            evaluations=evals,
            score=score,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary."""
        return {
            "screener_name": self.screener_name,
            "logic": self.logic,
            "conditions": {
                "condition_1": {
                    "id": "condition_1",
                    "name": self.condition_1.name,
                    "enabled": self.condition_1.enabled,
                    "operator": self.condition_1.operator,
                    "factor": self.condition_1.factor,
                    "offset": self.condition_1.offset,
                },
                "condition_2": {
                    "id": "condition_2",
                    "name": self.condition_2.name,
                    "enabled": self.condition_2.enabled,
                    "operator": self.condition_2.operator,
                    "lookback_days": self.condition_2.lookback_days,
                    "factor": self.condition_2.factor,
                },
                "condition_3": {
                    "id": "condition_3",
                    "name": self.condition_3.name,
                    "enabled": self.condition_3.enabled,
                    "operator": self.condition_3.operator,
                    "lookback_days": self.condition_3.lookback_days,
                    "factor": self.condition_3.factor,
                },
                "condition_4": {
                    "id": "condition_4",
                    "name": self.condition_4.name,
                    "enabled": self.condition_4.enabled,
                    "operator": self.condition_4.operator,
                    "rsi_window": self.condition_4.rsi_window,
                    "lookback_days": self.condition_4.lookback_days,
                    "rsi_delta": self.condition_4.rsi_delta,
                },
                "condition_5": {
                    "id": "condition_5",
                    "name": self.condition_5.name,
                    "enabled": self.condition_5.enabled,
                    "operator": self.condition_5.operator,
                    "min_volume": self.condition_5.min_volume,
                },
            },
        }

    def save_json(self, path: Path | str) -> None:
        """Save configuration to JSON file."""
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load_json(cls, path: Path | str) -> ScreenerConditionsConfig:
        """Load configuration from JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        conds = data.get("conditions", {})
        c1_data = conds.get("condition_1", {})
        c2_data = conds.get("condition_2", {})
        c3_data = conds.get("condition_3", {})
        c4_data = conds.get("condition_4", {})
        c5_data = conds.get("condition_5", {})

        return cls(
            screener_name=data.get("screener_name", "Chartink Momentum Screener"),
            logic=data.get("logic", "AND"),
            condition_1=Condition1HourlyAboveDailyOpen(
                enabled=c1_data.get("enabled", True),
                factor=c1_data.get("factor", 1.0),
                offset=c1_data.get("offset", 0.0),
                operator=c1_data.get("operator", ">"),
            ),
            condition_2=Condition2HourlyAbovePrevCloseDiscount(
                enabled=c2_data.get("enabled", True),
                lookback_days=c2_data.get("lookback_days", 1),
                factor=c2_data.get("factor", 0.95),
                operator=c2_data.get("operator", ">"),
            ),
            condition_3=Condition3HourlyAbovePastClose(
                enabled=c3_data.get("enabled", True),
                lookback_days=c3_data.get("lookback_days", 10),
                factor=c3_data.get("factor", 1.0),
                operator=c3_data.get("operator", ">="),
            ),
            condition_4=Condition4DailyRSIJump(
                enabled=c4_data.get("enabled", True),
                rsi_window=c4_data.get("rsi_window", 14),
                lookback_days=c4_data.get("lookback_days", 1),
                rsi_delta=c4_data.get("rsi_delta", 10.0),
                operator=c4_data.get("operator", ">"),
            ),
            condition_5=Condition5DailyVolumeThreshold(
                enabled=c5_data.get("enabled", False),
                min_volume=c5_data.get("min_volume", 5000000.0),
                operator=c5_data.get("operator", ">"),
            ),
        )
