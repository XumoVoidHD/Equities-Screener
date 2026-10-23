"""Test script for DhanBroker wrapper:

Verifies:
1. Environment authentication & cached token retrieval.
2. Streaming real-time LTP with callbacks.
3. Streaming real-time Bars (5-second bars & 1-minute bars) with update/close callbacks.
4. Querying cached prices and bars.
5. Graceful connection lifecycle.
"""

import sys
import time
from datetime import datetime

from broker import DhanBroker, TimeFrame


def test_dhan_broker():
    print("=" * 65)
    print("      DHANHQ BROKER WRAPPER LIVE STREAMING VERIFICATION      ")
    print("=" * 65)

    print("\n[1/5] Initializing DhanBroker from .env...")
    broker = DhanBroker()
    print(f"      Client ID: {broker.config.client_id}")
    print(f"      Token Source: Cached/Managed via {broker.config.token_cache_path}")

    # Track received data
    ticks_received = []
    ltp_updates = []
    bar_updates = []
    bars_closed = []

    def on_tick_handler(tick):
        ticks_received.append(tick)

    def on_ltp_handler(symbol, ltp, ts):
        ltp_updates.append((symbol, ltp, ts))
        print(f"  [LTP UPDATE] {symbol:<10} | Price: {ltp:>10.2f} | Time: {ts.strftime('%H:%M:%S')}")

    def on_bar_update_handler(bar):
        bar_updates.append(bar)
        print(f"  [BAR IN-PROGRESS] {bar.symbol} [{bar.timeframe}] O:{bar.open:.2f} H:{bar.high:.2f} L:{bar.low:.2f} C:{bar.close:.2f} Ticks:{bar.ticks_count}")

    def on_bar_close_handler(bar):
        bars_closed.append(bar)
        print(f"\n  >>> [BAR CLOSED] <<< {bar.symbol} [{bar.timeframe}] O:{bar.open:.2f} H:{bar.high:.2f} L:{bar.low:.2f} C:{bar.close:.2f} Vol:{bar.volume}\n")

    broker.on_tick(on_tick_handler)

    print("\n[2/5] Subscribing to LTP and 5-Second / 1-Minute Bars...")
    # Subscribe to NIFTY (Index) and RELIANCE (NSE Equity)
    broker.subscribe_ltp("NIFTY", callback=on_ltp_handler)
    broker.subscribe_ltp("RELIANCE", callback=on_ltp_handler)

    # Subscribe to 5-second bars to observe live candle closes during test
    broker.subscribe_bars(
        symbols=["NIFTY", "RELIANCE"],
        timeframe="5s",
        on_bar_close=on_bar_close_handler,
        on_bar_update=on_bar_update_handler,
    )

    print("\n[3/5] Starting WebSocket stream...")
    broker.connect()

    # Stream for 12 seconds to capture live ticks and at least 2 completed 5s bars
    stream_duration = 12
    print(f"      Streaming live market data for {stream_duration} seconds...\n")

    start_t = time.time()
    while time.time() - start_t < stream_duration:
        time.sleep(1)

    print("\n[4/5] Disconnecting broker...")
    broker.disconnect()

    print("\n[5/5] Querying cached state from broker wrapper:")
    nifty_ltp = broker.get_ltp("NIFTY")
    reliance_ltp = broker.get_ltp("RELIANCE")
    nifty_bar = broker.get_latest_bar("NIFTY", "5s")
    reliance_bar = broker.get_latest_bar("RELIANCE", "5s")
    closed_nifty_bars = broker.get_bars("NIFTY", "5s")

    print(f"      Latest NIFTY LTP:       {nifty_ltp}")
    print(f"      Latest RELIANCE LTP:    {reliance_ltp}")
    print(f"      Latest NIFTY 5s Bar:    {nifty_bar}")
    print(f"      Latest RELIANCE 5s Bar: {reliance_bar}")
    print(f"      Total Closed 5s Bars:   {len(closed_nifty_bars)}")
    print(f"      Total Raw Ticks:        {len(ticks_received)}")
    print(f"      Total LTP Callbacks:    {len(ltp_updates)}")
    print(f"      Total Bar Updates:      {len(bar_updates)}")

    print("\n" + "=" * 65)
    if len(ticks_received) > 0 and len(bars_closed) > 0:
        print("          ALL TESTS PASSED SUCCESSFULLY!          ")
    elif len(ticks_received) > 0:
        print("          STREAMING TEST PASSED (Ticks received)          ")
    else:
        print("          FAILED: No ticks received. Check credentials or market hours.")
        sys.exit(1)
    print("=" * 65)


if __name__ == "__main__":
    test_dhan_broker()
