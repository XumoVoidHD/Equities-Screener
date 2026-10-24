"""DhanHQ Broker Wrapper for streaming LTP and Bars.

Provides:
    - DhanBroker: High-level broker wrapper with automatic authentication,
      streaming LTP, real-time bar aggregation, and historical data fetching.
    - Models: Tick, Bar, ExchangeSegment, FeedMode, TimeFrame
    - BarAggregator: Real-time tick-to-bar aggregator (1s, 5s, 1m, 5m, etc.)
"""

from broker.models import Bar, ExchangeSegment, FeedMode, Tick, TimeFrame
from broker.client import DhanBroker
from broker.bar_aggregator import BarAggregator
from broker.scrip_master import ScripMaster
from broker.conditions import (
    StockMarketState,
    ConditionEvaluationResult,
    ScreenerResult,
    ScreenerConditionsConfig,
    Condition1HourlyAboveDailyOpen,
    Condition2HourlyAbovePrevCloseDiscount,
    Condition3HourlyAbovePastClose,
    Condition4DailyRSIJump,
    Condition5DailyVolumeThreshold,
    compute_wilder_rsi,
)
from broker.nautilus_screener import NautilusEquityScreenerActor, ScreenerConfig, ScreenerMatch

__all__ = [
    "DhanBroker",
    "Tick",
    "Bar",
    "ExchangeSegment",
    "FeedMode",
    "TimeFrame",
    "BarAggregator",
    "ScripMaster",
    "NautilusEquityScreenerActor",
    "ScreenerConfig",
    "ScreenerMatch",
    "StockMarketState",
    "ConditionEvaluationResult",
    "ScreenerResult",
    "ScreenerConditionsConfig",
    "Condition1HourlyAboveDailyOpen",
    "Condition2HourlyAbovePrevCloseDiscount",
    "Condition3HourlyAbovePastClose",
    "Condition4DailyRSIJump",
    "Condition5DailyVolumeThreshold",
    "compute_wilder_rsi",
]

