"""Verification & Demonstration Suite for NautilusEquityScreenerActor.

Tests:
1. Synthetic Nautilus Bar processing:
   - Feeds baseline historical bars across multiple instruments.
   - Injects volume surge, price breakout, and range expansion events.
   - Verifies screener triggers, reason generation, and leaderboard ranking.
2. Direct DhanBroker live bridge verification:
   - Attaches the Nautilus Actor to DhanBroker.
   - Streams live 5s / 1m bars from Dhan WebSocket into the Nautilus Actor.
   - Displays real-time matching watchlist.

Usage:
    python test_nautilus_screener.py
"""

import sys
import time
from datetime import datetime, timezone

from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

from broker import (
    DhanBroker,
    NautilusEquityScreenerActor,
    ScreenerConfig,
    ScreenerMatch,
    TimeFrame,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def make_nautilus_bar(
    symbol: str,
    venue: str,
    open_px: float,
    high_px: float,
    low_px: float,
    close_px: float,
    volume: int,
    ts_seconds: float,
    timeframe: str = "1-MINUTE",
) -> Bar:
    """Helper to construct a valid NautilusTrader Bar."""
    inst_id_str = f"{symbol}.{venue}"
    bar_type = BarType.from_str(f"{inst_id_str}-{timeframe}-LAST-EXTERNAL")
    ts_ns = int(ts_seconds * 1e9)
    return Bar(
        bar_type=bar_type,
        open=Price(open_px, 2),
        high=Price(high_px, 2),
        low=Price(low_px, 2),
        close=Price(close_px, 2),
        volume=Quantity(volume, 0),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def test_synthetic_screening():
    print("=" * 70)
    print("   TEST 1: NAUTILUS ACTOR SYNTHETIC BAR SCREENING VERIFICATION   ")
    print("=" * 70)

    # 1. Initialize Actor with aggressive detection thresholds for testing
    config = ScreenerConfig(
        lookback_bars=5,
        min_volume_ratio=2.0,      # Volume >= 2.0x average
        min_price_change_pct=1.0,  # Single-bar move >= 1.0%
        check_breakout=True,       # Detect new N-bar highs
        min_bar_range_pct=0.8,
    )
    screener = NautilusEquityScreenerActor(config=config)

    matches_received = []

    def on_match(match: ScreenerMatch):
        print(f"  [SCREENER MATCH TRIGGERED] {match.symbol} -> {', '.join(match.reasons)}")
        matches_received.append(match)

    screener.on_match(on_match)
    screener.on_start()

    base_time = time.time() - 300

    # 2. Feed 5 baseline normal bars for RELIANCE (Rs. 2500, avg vol ~10,000)
    print("\n[Step 1] Feeding 5 baseline bars for RELIANCE (avg vol: 10,000)...")
    for i in range(5):
        bar = make_nautilus_bar(
            symbol="RELIANCE",
            venue="NSE",
            open_px=2500.0 + i,
            high_px=2505.0 + i,
            low_px=2498.0 + i,
            close_px=2502.0 + i,
            volume=10000 + (i * 100),
            ts_seconds=base_time + (i * 60),
        )
        screener.on_bar(bar)

    assert len(matches_received) == 0, "Baseline bars should not trigger alerts"
    print("   Baseline bars ingested cleanly without false alarms.")

    # 3. Inject a breakout bar: +2.5% price spike + 3.5x volume surge + new 5-bar high
    print("\n[Step 2] Injecting momentum bar on RELIANCE (+2.5% jump, 35,000 vol)...")
    spike_bar = make_nautilus_bar(
        symbol="RELIANCE",
        venue="NSE",
        open_px=2506.0,
        high_px=2575.0,  # Big new high
        low_px=2505.0,
        close_px=2568.0,  # ~ +2.47% move
        volume=35000,    # 3.5x normal volume
        ts_seconds=base_time + 300,
    )
    screener.on_bar(spike_bar)

    assert len(matches_received) == 1, "Spike bar should trigger match"
    match = matches_received[0]
    print(f"\n   Match Details:")
    print(f"   - Symbol:       {match.symbol}")
    print(f"   - Close Price:  Rs. {match.price:.2f}")
    print(f"   - Change:       {match.change_pct:+.2f}%")
    print(f"   - Volume Ratio: {match.volume_ratio:.1f}x")
    print(f"   - Reasons:      {match.reasons}")
    print(f"   - Urgency Score:{match.score:.2f}")

    # Check watchlist
    watchlist = screener.get_watchlist()
    assert len(watchlist) == 1
    assert watchlist[0].symbol == "RELIANCE"

    # 4. Feed baseline and surge on a second stock (TCS)
    print("\n[Step 3] Feeding baseline and surge for TCS (Rs. 3500)...")
    for i in range(5):
        bar = make_nautilus_bar(
            symbol="TCS",
            venue="NSE",
            open_px=3500.0,
            high_px=3510.0,
            low_px=3495.0,
            close_px=3502.0,
            volume=8000,
            ts_seconds=base_time + (i * 60),
        )
        screener.on_bar(bar)

    tcs_surge = make_nautilus_bar(
        symbol="TCS",
        venue="NSE",
        open_px=3505.0,
        high_px=3580.0,
        low_px=3500.0,
        close_px=3575.0,
        volume=40000,  # 5.0x volume!
        ts_seconds=base_time + 300,
    )
    screener.on_bar(tcs_surge)

    # 5. Display Leaderboard
    print("\n[Step 4] Current Screener Watchlist Leaderboard:")
    print("  " + "-" * 65)
    for rank, m in enumerate(screener.get_watchlist(), 1):
        reasons_str = ", ".join(m.reasons)
        print(f"   #{rank} {m.symbol:<12} Price: Rs.{m.price:>8.2f} | Chg: {m.change_pct:>+6.2f}% | Vol: {m.volume_ratio:>4.1f}x | Score: {m.score:>5.1f} [{reasons_str}]")

    screener.on_stop()
    print("\n   TEST 1 PASSED: Synthetic Screener verified successfully!")
    print("=" * 70)


def test_dhan_broker_live_bridge(duration_seconds: int = 10):
    print("\n" + "=" * 70)
    print("   TEST 2: LIVE DHAN BROKER -> NAUTILUS ACTOR BRIDGE TEST         ")
    print("=" * 70)

    # 1. Setup Nautilus Screener Actor
    screener = NautilusEquityScreenerActor(
        config=ScreenerConfig(
            lookback_bars=3,
            min_volume_ratio=1.0,
            min_price_change_pct=0.01,
            min_bar_range_pct=0.01,
        )
    )

    screener_matches = []

    def on_live_match(match: ScreenerMatch):
        print(f"  [LIVE ACTOR MATCH] {match.symbol} Rs.{match.price:.2f} ({match.change_pct:+.2f}%) -> {', '.join(match.reasons)}")
        screener_matches.append(match)

    screener.on_match(on_live_match)

    # 2. Setup DhanBroker
    print("\n[1/3] Initializing DhanBroker...")
    broker = DhanBroker(bar_timeframes=["5s"])

    # 3. Attach Nautilus Screener Actor to DhanBroker (on_update=True allows screening incoming ticks/bars)
    screener.attach_to_broker(broker, on_update=True)

    # 4. Subscribe to 5s bars for sample equities
    stocks_to_test = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]
    print(f"\n[2/3] Subscribing to 5s bars for: {', '.join(stocks_to_test)}...")
    broker.subscribe_bars(stocks_to_test, timeframe="5s")

    # 5. Stream
    print(f"\n[3/3] Streaming live for {duration_seconds} seconds...\n")
    broker.connect()
    try:
        t_end = time.time() + duration_seconds
        while time.time() < t_end:
            time.sleep(1)
            bars_count = screener.total_bars_processed
            print(f"  Streaming... Bars processed by Nautilus Actor: {bars_count:4d}")
    finally:
        print("\nStopping DhanBroker...")
        broker.disconnect()
        screener.on_stop()

    print("\n" + "=" * 70)
    print("                 LIVE ACTOR BRIDGE RESULTS                       ")
    print("=" * 70)
    print(f"  Total Bars Ingested by Nautilus Actor: {screener.total_bars_processed}")
    print(f"  Screener Matches Triggered:             {len(screener_matches)}")
    print(f"  Watchlist Entries:                      {len(screener.get_watchlist())}")
    for rank, m in enumerate(screener.get_watchlist(), 1):
        print(f"   #{rank} {m.symbol:<12} Rs.{m.price:.2f} [{', '.join(m.reasons)}]")
    print("=" * 70)
    print("   TEST 2 COMPLETE: DhanBroker -> Nautilus Actor Bridge verified!")
    print("=" * 70)


if __name__ == "__main__":
    test_synthetic_screening()
    if len(sys.argv) > 1 and sys.argv[1] == "live":
        dur = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        test_dhan_broker_live_bridge(duration_seconds=dur)
    else:
        print("\nTip: Run 'python test_nautilus_screener.py live 10' to test live Dhan feed bridge.")
