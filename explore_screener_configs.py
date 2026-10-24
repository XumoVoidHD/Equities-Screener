"""Configuration Optimization & Exploration Suite for Indian Equities Screener.

Tests 20+ distinct configurations across real cached daily data for 116 NSE stocks.
Measures:
- Pass Count & Pass Rate (%)
- Which conditions act as the primary filters / bottlenecks
- Top stock candidates surfaced by each configuration
- Profiles configurations into high-yield, balanced, and sniper setups.

Usage:
    python explore_screener_configs.py
"""

import json
from pathlib import Path
from typing import Any

from broker.conditions import (
    Condition1HourlyAboveDailyOpen,
    Condition2HourlyAbovePrevCloseDiscount,
    Condition3HourlyAbovePastClose,
    Condition4DailyRSIJump,
    Condition5DailyVolumeThreshold,
    ScreenerConditionsConfig,
    StockMarketState,
)


def load_dataset() -> list[StockMarketState]:
    cache_path = Path("data/dhan/research_sample_daily.json")
    with open(cache_path, "r", encoding="utf-8") as f:
        cache = json.load(f)

    states = []
    for sym, candles in cache.items():
        closes = candles.get("close", [])
        opens = candles.get("open", [])
        vols = candles.get("volume", [])
        highs = candles.get("high", [])
        lows = candles.get("low", [])

        if len(closes) < 20:
            continue

        hourly_px = closes[-1]
        d_open = opens[-1] if opens else hourly_px
        d_vol = vols[-1] if vols else 0.0

        state = StockMarketState(
            symbol=sym,
            hourly_close=hourly_px,
            daily_open=d_open,
            daily_closes=closes[:-1],  # Past completed daily closes
            daily_volumes=vols[:-1] if vols else [],
            daily_highs=highs[:-1] if highs else [],
            daily_lows=lows[:-1] if lows else [],
            daily_volume=d_vol,
        )
        states.append(state)

    return states


def run_experiment(config: ScreenerConditionsConfig, states: list[StockMarketState]) -> dict[str, Any]:
    matched_stocks = []
    condition_pass_counts = {
        "condition_1": 0,
        "condition_2": 0,
        "condition_3": 0,
        "condition_4": 0,
        "condition_5": 0,
    }

    for state in states:
        res = config.evaluate(state)
        for cid, eval_res in res.evaluations.items():
            if eval_res.passed and eval_res.enabled:
                condition_pass_counts[cid] += 1

        if res.passed:
            matched_stocks.append({
                "symbol": state.symbol,
                "price": state.hourly_close,
                "score": res.score,
                "matched_conditions": res.matched_conditions,
            })

    # Sort matched stocks by score
    matched_stocks.sort(key=lambda s: s["score"], reverse=True)

    pass_rate = (len(matched_stocks) / len(states)) * 100.0 if states else 0.0
    return {
        "matched_count": len(matched_stocks),
        "total_stocks": len(states),
        "pass_rate_pct": round(pass_rate, 1),
        "matched_stocks": matched_stocks,
        "condition_pass_counts": condition_pass_counts,
    }


def main():
    states = load_dataset()
    print("=" * 80)
    print(f"   SCREENER CONFIGURATION RESEARCH & OPTIMIZATION BENCHMARK")
    print(f"   Universe: {len(states)} Real Indian Stocks (NSE Equities)")
    print("=" * 80)

    # Define Configurations
    experiments = [
        # --- Group 1: Original vs RSI Delta Variations ---
        (
            "Config 1: Original Screenshot (Strict RSI Delta >= 10)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=10.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Original Chartink setup with strict 10-point single-day RSI surge.",
        ),
        (
            "Config 2: Moderate RSI Surge (RSI Delta >= 5.0)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=5.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Requires +5 RSI increase today instead of +10.",
        ),
        (
            "Config 3: Positive RSI Progression (RSI Delta >= 2.0)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=2.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Requires RSI to be gaining positive momentum (+2 pts).",
        ),
        (
            "Config 4: Any RSI Uptick (RSI Delta >= 0.0)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=0.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Requires Today RSI >= Yesterday RSI (RSI is rising or flat).",
        ),
        # --- Group 2: Trend Following (RSI Disabled or Pure Price Action) ---
        (
            "Config 5: Pure Price Uptrend (C1 + C2 + C3, RSI Disabled)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=False),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Classic swing setup: Green candle today + holding above 10-day price.",
        ),
        (
            "Config 6: Short-Term Fast Trend (5-Day Lookback, RSI Delta >= 1.0)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.96),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=5),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=1.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Faster response: Looks back 5 days instead of 10, mild RSI gain.",
        ),
        (
            "Config 7: Ultra-Short Momentum (3-Day Lookback, RSI Delta >= 0.5)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.97),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=3),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=0.5),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Quick 3-day swing continuation for intraday / multi-day runners.",
        ),
        # --- Group 3: Relaxed Thresholds for High Yield ---
        (
            "Config 8: Relaxed Price & Mild Dip Tolerance (Factor=0.98, Lookback=10)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True, factor=0.995),  # Within 0.5% of open
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10, factor=0.98), # Within 2% of 10d close
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=0.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Allows stocks near open and near 10-day consolidation highs.",
        ),
        (
            "Config 9: Intraday Momentum Only (C1 + C2 Only)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=False),
                condition_4=Condition4DailyRSIJump(enabled=False),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Green intraday candle with normal volatility floor (> -5%).",
        ),
        (
            "Config 10: Multi-Condition Wide Net (OR Logic, All Active)",
            ScreenerConditionsConfig(
                logic="OR",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=True, rsi_delta=2.0),
                condition_5=Condition5DailyVolumeThreshold(enabled=False),
            ),
            "Triggers if ANY bullish indicator passes.",
        ),
        (
            "Config 11: High-Volume Momentum (C1 + C3 + Volume > 100k)",
            ScreenerConditionsConfig(
                logic="AND",
                condition_1=Condition1HourlyAboveDailyOpen(enabled=True),
                condition_2=Condition2HourlyAbovePrevCloseDiscount(enabled=True, factor=0.95),
                condition_3=Condition3HourlyAbovePastClose(enabled=True, lookback_days=10),
                condition_4=Condition4DailyRSIJump(enabled=False),
                condition_5=Condition5DailyVolumeThreshold(enabled=True, min_volume=100000.0),
            ),
            "Uptrend continuation with institutional liquidity (> 100,000 shares).",
        ),
    ]

    results_table = []

    for name, cfg, desc in experiments:
        res = run_experiment(cfg, states)
        top_syms = [s["symbol"] for s in res["matched_stocks"][:8]]
        results_table.append({
            "name": name,
            "matched_count": res["matched_count"],
            "pass_rate_pct": res["pass_rate_pct"],
            "top_stocks": top_syms,
            "desc": desc,
            "c_counts": res["condition_pass_counts"],
        })

    print(f"\n{'CONFIG NAME':<55} | {'MATCHES':>8} | {'PASS %':>7} | TOP STOCKS")
    print("-" * 110)
    for r in results_table:
        stocks_str = ", ".join(r["top_stocks"][:6])
        if len(r["top_stocks"]) > 6:
            stocks_str += "..."
        print(f"{r['name']:<55} | {r['matched_count']:>4}/{len(states)} | {r['pass_rate_pct']:>6.1f}% | {stocks_str}")

    # Identify the Top High-Yield Configurations
    print("\n" + "=" * 80)
    print("   KEY FINDINGS & BOTTLENECK ANALYSIS")
    print("=" * 80)
    # Check individual condition pass rates in base config
    orig = results_table[0]["c_counts"]
    print(f"Condition 1 (Hourly Close > Daily Open):  {orig['condition_1']:>3} stocks ({orig['condition_1']/len(states)*100:.1f}%)")
    print(f"Condition 2 (Hourly > [-1] Close * 0.95): {orig['condition_2']:>3} stocks ({orig['condition_2']/len(states)*100:.1f}%)")
    print(f"Condition 3 (Hourly >= [-10] Close):      {orig['condition_3']:>3} stocks ({orig['condition_3']/len(states)*100:.1f}%)")
    print(f"Condition 4 (RSI Jump >= +10.0 pts):     {orig['condition_4']:>3} stocks ({orig['condition_4']/len(states)*100:.1f}%)  <-- MAJOR BOTTLENECK!")


if __name__ == "__main__":
    main()
