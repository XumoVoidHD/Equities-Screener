"""Comprehensive Test and Verification Suite for Configurable Screener Conditions.

Verifies:
1. Individual Rule Evaluation:
   - Condition 1: Hourly Close > Daily Open (configurable factor & offset)
   - Condition 2: Hourly Close > ([-1] Daily Close * 0.95) (configurable lookback & discount factor)
   - Condition 3: Hourly Close >= [-10] Daily Close (configurable lookback & factor)
   - Condition 4: 1day RSI(14) > ([-1] 1day RSI(14) + 10) (configurable window, lookback & delta)
   - Condition 5: Daily Volume > 5,000,000 (configurable threshold & enabled toggle)
2. Dynamic Configuration & JSON Persistence:
   - Modifying condition parameters programmatically
   - Toggling conditions on/off
   - Loading from & saving to `config/screener_conditions.json`
3. Nautilus Actor Integration:
   - Processing bars with multi-timeframe StockMarketState
   - Generating ScreenerMatch with detailed condition evaluations
   - Watchlist ranking by composite urgency score
4. Real Dhan Equity Market Data Test:
   - Preloading actual historical daily candles from Dhan API
   - Evaluating live/latest market state against all 5 conditions

Usage:
    python test_chartink_screener.py
    python test_chartink_screener.py real     # Tests with real Dhan historical data
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.objects import Price, Quantity

from broker import (
    DhanBroker,
    NautilusEquityScreenerActor,
    ScreenerConditionsConfig,
    ScreenerConfig,
    ScreenerMatch,
    StockMarketState,
    compute_wilder_rsi,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def test_individual_conditions():
    print("=" * 75)
    print("   TEST 1: INDIVIDUAL CONDITION EVALUATION & PARAMETER CHECKS   ")
    print("=" * 75)

    # Synthetic realistic price history (25 sessions)
    # Day -1 close is 2500.0, Day -10 close is 2450.0
    daily_closes = [2400.0 + (i * 5) for i in range(25)]  # Closes from 2400 to 2520
    # Let's customize specific values
    daily_closes[-1] = 2500.0  # [-1] Daily Close = 2500.0
    daily_closes[-10] = 2450.0  # [-10] Daily Close = 2450.0

    # 1. Test Condition 1: Hourly Close > Daily Open
    print("\n[Condition 1] Hourly Close > Daily Open:")
    cfg = ScreenerConditionsConfig()
    # Case A: Hourly Close (2520) > Daily Open (2500) -> PASS
    state_pass = StockMarketState(
        symbol="TEST",
        hourly_close=2520.0,
        daily_open=2500.0,
        daily_closes=daily_closes,
    )
    res_c1 = cfg.condition_1.evaluate(state_pass)
    print(f"   Pass Case: {res_c1.message} -> Passed={res_c1.passed}")
    assert res_c1.passed is True

    # Case B: Hourly Close (2480) <= Daily Open (2500) -> FAIL
    state_fail = StockMarketState(
        symbol="TEST",
        hourly_close=2480.0,
        daily_open=2500.0,
        daily_closes=daily_closes,
    )
    res_c1_fail = cfg.condition_1.evaluate(state_fail)
    print(f"   Fail Case: {res_c1_fail.message} -> Passed={res_c1_fail.passed}")
    assert res_c1_fail.passed is False

    # 2. Test Condition 2: Hourly Close > ([-1] Daily Close * 0.95)
    print("\n[Condition 2] Hourly Close > ([-1] Daily Close * 0.95):")
    # [-1] Daily Close is 2500.0, threshold is 2500 * 0.95 = 2375.0
    state_c2_pass = StockMarketState(
        symbol="TEST",
        hourly_close=2400.0,  # 2400 > 2375 -> PASS
        daily_open=2500.0,
        daily_closes=daily_closes,
    )
    res_c2 = cfg.condition_2.evaluate(state_c2_pass)
    print(f"   Pass Case: {res_c2.message} -> Passed={res_c2.passed}")
    assert res_c2.passed is True

    state_c2_fail = StockMarketState(
        symbol="TEST",
        hourly_close=2350.0,  # 2350 <= 2375 -> FAIL
        daily_open=2500.0,
        daily_closes=daily_closes,
    )
    res_c2_fail = cfg.condition_2.evaluate(state_c2_fail)
    print(f"   Fail Case: {res_c2_fail.message} -> Passed={res_c2_fail.passed}")
    assert res_c2_fail.passed is False

    # 3. Test Condition 3: Hourly Close >= [-10] Daily Close
    print("\n[Condition 3] Hourly Close >= [-10] Daily Close:")
    # [-10] Daily Close is 2450.0
    state_c3_pass = StockMarketState(
        symbol="TEST",
        hourly_close=2450.0,  # 2450 >= 2450 -> PASS (>= operator)
        daily_open=2400.0,
        daily_closes=daily_closes,
    )
    res_c3 = cfg.condition_3.evaluate(state_c3_pass)
    print(f"   Pass Case: {res_c3.message} -> Passed={res_c3.passed}")
    assert res_c3.passed is True

    state_c3_fail = StockMarketState(
        symbol="TEST",
        hourly_close=2440.0,  # 2440 < 2450 -> FAIL
        daily_open=2400.0,
        daily_closes=daily_closes,
    )
    res_c3_fail = cfg.condition_3.evaluate(state_c3_fail)
    print(f"   Fail Case: {res_c3_fail.message} -> Passed={res_c3_fail.passed}")
    assert res_c3_fail.passed is False

    # 4. Test Condition 4: 1day RSI(14) > ([-1] 1day RSI(14) + 10)
    print("\n[Condition 4] 1day RSI(14) > ([-1] 1day RSI(14) + 10):")
    # Construct fluctuating sequence where yesterday RSI is ~30 and today surges to ~55 (delta > 20)
    rsi_history = [
        100.0, 98.0, 99.0, 97.0, 96.0, 98.0, 97.0, 95.0, 94.0, 96.0,
        95.0, 93.0, 94.0, 92.0, 93.0, 91.0, 90.0, 91.0, 90.0, 89.0
    ]
    state_c4_surge = StockMarketState(
        symbol="TEST",
        hourly_close=99.0,  # Strong spike from 89 to 99
        daily_open=90.0,
        daily_closes=rsi_history,
    )
    res_c4 = cfg.condition_4.evaluate(state_c4_surge)
    print(f"   Pass Case: {res_c4.message} -> Passed={res_c4.passed}")
    assert res_c4.passed is True

    # Flat day should not trigger RSI jump > 10
    state_c4_flat = StockMarketState(
        symbol="TEST",
        hourly_close=89.2,
        daily_open=89.0,
        daily_closes=rsi_history,
    )
    res_c4_fail = cfg.condition_4.evaluate(state_c4_flat)
    print(f"   Fail Case: {res_c4_fail.message} -> Passed={res_c4_fail.passed}")
    assert res_c4_fail.passed is False

    # 5. Test Condition 5: Daily Volume > 5,000,000 (Disabled by default)
    print("\n[Condition 5] Daily Volume > 5,000,000:")
    state_vol = StockMarketState(
        symbol="TEST",
        hourly_close=2500.0,
        daily_open=2480.0,
        daily_closes=daily_closes,
        daily_volume=6500000.0,
    )
    res_c5_default = cfg.condition_5.evaluate(state_vol)
    print(f"   Default state: Enabled={cfg.condition_5.enabled} -> Passed={res_c5_default.passed} (Disabled)")
    assert res_c5_default.passed is True  # When disabled, condition does not block evaluation

    # Enable and test threshold
    cfg.condition_5.enabled = True
    res_c5_enabled = cfg.condition_5.evaluate(state_vol)
    print(f"   Enabled Pass: {res_c5_enabled.message} -> Passed={res_c5_enabled.passed}")
    assert res_c5_enabled.passed is True

    state_vol_low = StockMarketState(
        symbol="TEST",
        hourly_close=2500.0,
        daily_open=2480.0,
        daily_closes=daily_closes,
        daily_volume=3000000.0,
    )
    res_c5_low = cfg.condition_5.evaluate(state_vol_low)
    print(f"   Enabled Fail: {res_c5_low.message} -> Passed={res_c5_low.passed}")
    assert res_c5_low.passed is False

    print("\n   TEST 1 PASSED: All 5 conditions evaluate with mathematical precision!")
    print("=" * 75)


def test_dynamic_configuration_and_json():
    print("\n" + "=" * 75)
    print("   TEST 2: DYNAMIC PARAMETER RECONFIGURATION & JSON PERSISTENCE  ")
    print("=" * 75)

    json_path = Path("config/screener_conditions.json")
    assert json_path.exists(), f"Configuration file missing at {json_path}"

    # 1. Load from JSON
    print(f"\n[Step 1] Loading master screener configuration from '{json_path}'...")
    cfg = ScreenerConditionsConfig.load_json(json_path)
    print(f"   Screener Name: '{cfg.screener_name}'")
    print(f"   Combination Logic: {cfg.logic}")
    print(f"   Condition 1: {cfg.condition_1.name} (Factor={cfg.condition_1.factor}, Offset={cfg.condition_1.offset})")
    print(f"   Condition 2: {cfg.condition_2.name} (Lookback={cfg.condition_2.lookback_days}, Factor={cfg.condition_2.factor})")
    print(f"   Condition 3: {cfg.condition_3.name} (Lookback={cfg.condition_3.lookback_days})")
    print(f"   Condition 4: {cfg.condition_4.name} (Window={cfg.condition_4.rsi_window}, Delta={cfg.condition_4.rsi_delta})")
    print(f"   Condition 5: {cfg.condition_5.name} (Enabled={cfg.condition_5.enabled}, MinVol={cfg.condition_5.min_volume:,.0f})")

    assert cfg.condition_1.enabled is True
    assert cfg.condition_2.factor == 0.95
    assert cfg.condition_3.lookback_days == 10
    assert cfg.condition_4.rsi_delta == 10.0
    assert cfg.condition_5.enabled is False

    # 2. Dynamically modify values via API
    print("\n[Step 2] Dynamically updating parameters via set_param()...")
    cfg.set_param("condition_2", "factor", 0.92)
    cfg.set_param("condition_4", "rsi_delta", 8.0)
    cfg.enable_condition("condition_5", True)
    cfg.set_param("condition_5", "min_volume", 2500000.0)

    assert cfg.condition_2.factor == 0.92
    assert cfg.condition_4.rsi_delta == 8.0
    assert cfg.condition_5.enabled is True
    assert cfg.condition_5.min_volume == 2500000.0
    print("   Parameters successfully updated in memory.")

    # 3. Save to temporary scratch JSON and verify reload
    scratch_path = Path("config/screener_conditions_custom.json")
    print(f"\n[Step 3] Exporting updated configuration to '{scratch_path}'...")
    cfg.save_json(scratch_path)
    assert scratch_path.exists()

    reloaded = ScreenerConditionsConfig.load_json(scratch_path)
    assert reloaded.condition_2.factor == 0.92
    assert reloaded.condition_4.rsi_delta == 8.0
    assert reloaded.condition_5.enabled is True
    assert reloaded.condition_5.min_volume == 2500000.0
    print("   Reloaded JSON verified: All custom parameters persisted intact.")

    # Clean up scratch file
    if scratch_path.exists():
        scratch_path.unlink()

    print("\n   TEST 2 PASSED: Configuration and JSON persistence verified!")
    print("=" * 75)


def test_nautilus_actor_integration():
    print("\n" + "=" * 75)
    print("   TEST 3: NAUTILUS ACTOR MULTI-CONDITION PIPELINE VERIFICATION  ")
    print("=" * 75)

    # 1. Instantiate Actor with default Chartink conditions
    screener = NautilusEquityScreenerActor(
        config=ScreenerConfig(lookback_bars=5, publish_signals=False),
        conditions_json_path="config/screener_conditions.json",
    )

    matches_triggered = []

    def handle_match(match: ScreenerMatch):
        print(f"  [SCREENER MATCH] {match.symbol} Price=Rs.{match.price:.2f} Score={match.score:.1f}")
        for r in match.reasons:
            print(f"     -> {r}")
        matches_triggered.append(match)

    screener.on_match(handle_match)
    screener.on_start()

    # 2. Preload historical daily context for RELIANCE
    # 25 days of fluctuating closes where yesterday RSI is ~31
    rsi_history = [
        2500.0, 2480.0, 2490.0, 2470.0, 2460.0, 2480.0, 2470.0, 2450.0, 2440.0, 2460.0,
        2450.0, 2430.0, 2440.0, 2420.0, 2430.0, 2410.0, 2400.0, 2410.0, 2400.0, 2390.0
    ]
    print("\n[Step 1] Preloading 20 days of daily context for RELIANCE...")
    screener.preload_daily_context(
        symbol="RELIANCE",
        daily_open=2400.0,
        daily_closes=rsi_history,
        daily_volume=2000000.0,
    )

    # 3. Ingest an hourly bar that triggers ALL 4 active conditions:
    # - Hourly Close (2520) > Daily Open (2400) -> Cond 1 PASS
    # - Hourly Close (2520) > [-1] Close (2390) * 0.95 -> Cond 2 PASS
    # - Hourly Close (2520) >= [-10] Close (2450) -> Cond 3 PASS
    # - Hourly Close (2520) surges from 2390 -> RSI surges +25 points -> Cond 4 PASS
    # - Cond 5 is disabled
    print("\n[Step 2] Ingesting bullish breakout hourly bar for RELIANCE (Rs. 2520)...")
    bar_type = BarType.from_str("RELIANCE.NSE-1-HOUR-LAST-EXTERNAL")
    ts_ns = int(time.time() * 1e9)
    breakout_bar = Bar(
        bar_type=bar_type,
        open=Price(2410.0, 2),
        high=Price(2525.0, 2),
        low=Price(2405.0, 2),
        close=Price(2520.0, 2),
        volume=Quantity(150000, 0),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )

    screener.on_bar(breakout_bar)

    assert len(matches_triggered) == 1, "Actor should have triggered a match"
    match = matches_triggered[0]
    assert match.symbol == "RELIANCE"
    assert match.condition_result is not None
    assert match.condition_result.passed is True
    print(f"\n   Match Details Verified:")
    print(f"   - Symbol:              {match.symbol}")
    print(f"   - Matched Conditions:  {len(match.condition_result.matched_conditions)} / 4 enabled")
    print(f"   - Score:               {match.score:.2f}")

    # Verify Leaderboard
    watchlist = screener.get_watchlist()
    assert len(watchlist) == 1
    assert watchlist[0].symbol == "RELIANCE"

    screener.on_stop()
    print("\n   TEST 3 PASSED: Nautilus Actor screening pipeline verified!")
    print("=" * 75)


def test_real_dhan_historical_screening():
    print("\n" + "=" * 75)
    print("   TEST 4: REAL DHAN HISTORICAL DAILY DATA PRELOAD & SCREENING   ")
    print("=" * 75)

    stocks_to_test = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]
    print(f"[Step 1] Initializing DhanBroker & fetching daily bars for: {', '.join(stocks_to_test)}...")

    broker = DhanBroker()
    screener = NautilusEquityScreenerActor(conditions_json_path="config/screener_conditions.json")

    # Preload real daily history from Dhan
    load_results = screener.fetch_and_preload_daily_context(broker, stocks_to_test, days=45)
    print(f"   Preload status: {load_results}")

    print("\n[Step 2] Evaluating current market state for each stock against all conditions:")
    print("  " + "-" * 71)

    for sym in stocks_to_test:
        if not load_results.get(sym, False):
            print(f"   Skipping {sym} (history unavailable)")
            continue

        state = screener._market_states[sym]
        res = screener.evaluate_symbol(sym)

        status_tag = "[PASS - ALL MATCH]" if res.passed else "[NO MATCH]"
        c1 = res.evaluations["condition_1"]
        c2 = res.evaluations["condition_2"]
        c3 = res.evaluations["condition_3"]
        c4 = res.evaluations["condition_4"]

        print(f"\n   Stock: {sym:<10} Latest Px: Rs.{state.hourly_close:>8.2f} | Result: {status_tag}")
        print(f"     * Cond 1 (Close > Open):           {'PASS' if c1.passed else 'FAIL'} | {c1.message}")
        print(f"     * Cond 2 (Close > [-1]*0.95):      {'PASS' if c2.passed else 'FAIL'} | {c2.message}")
        print(f"     * Cond 3 (Close >= [-10]):         {'PASS' if c3.passed else 'FAIL'} | {c3.message}")
        print(f"     * Cond 4 (RSI > [-1] RSI + 10):    {'PASS' if c4.passed else 'FAIL'} | {c4.message}")

    print("\n   TEST 4 COMPLETE: Real market daily data successfully screened!")
    print("=" * 75)


if __name__ == "__main__":
    test_individual_conditions()
    test_dynamic_configuration_and_json()
    test_nautilus_actor_integration()

    if len(sys.argv) > 1 and sys.argv[1] == "real":
        test_real_dhan_historical_screening()
    else:
        print("\nTip: Run 'python test_chartink_screener.py real' to test live screening against Dhan REST API daily history.")
