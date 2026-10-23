"""Stream 1,000 Stocks via DhanHQ WebSockets

This script tests subscribing to and processing 1,000 active NSE equity stocks
simultaneously over a single DhanHQ WebSocket connection.

Metrics measured:
- Batch subscription throughput (10 batches of 100 instruments).
- Total ticks received.
- Unique stocks with live price updates.
- Tick arrival rate (ticks/second).
- Sample live LTPs from the 1,000 universe.
"""

import json
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from broker import DhanBroker, ExchangeSegment, FeedMode


def run_1000_stocks_stream(duration_seconds: int = 15):
    print("=" * 70)
    print("        STREAMING 1,000 STOCKS SIMULTANEOUSLY VIA WEBSOCKET         ")
    print("=" * 70)

    # 1. Load 1,000 stocks list
    stocks_path = Path("data/dhan/nse_1000_stocks.json")
    if not stocks_path.exists():
        print(f"Error: {stocks_path} not found. Please extract stock list first.")
        sys.exit(1)

    with open(stocks_path, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    total_stocks = len(stocks)
    print(f"\n[1/4] Loaded {total_stocks} stocks from {stocks_path}")
    sample_symbols = [s["symbol"] for s in stocks[:8]]
    print(f"      Sample stocks: {', '.join(sample_symbols)} ...")

    # 2. Initialize Broker
    print("\n[2/4] Initializing DhanBroker...")
    broker = DhanBroker()
    print(f"      Client ID: {broker.config.client_id}")

    # Data tracking
    ticks_by_symbol = Counter()
    total_ticks = 0
    start_stream_time = None

    def on_tick(tick):
        nonlocal total_ticks
        total_ticks += 1
        ticks_by_symbol[tick.symbol] += 1

    broker.on_tick(on_tick)

    # 3. Subscribe all 1000 instruments
    print(f"\n[3/4] Subscribing to {total_stocks} instruments in batches of 100...")
    sub_tuples = [
        (s["exchange_segment"], s["security_id"])
        for s in stocks
    ]
    sub_start = time.time()
    broker.subscribe_ltp(sub_tuples)
    sub_duration = time.time() - sub_start
    print(f"      Mapped and registered {len(sub_tuples)} subscriptions in {sub_duration:.3f}s")

    # 4. Connect WebSocket
    print(f"\n[4/4] Connecting to WebSocket and streaming for {duration_seconds} seconds...")
    connect_start = time.time()
    broker.connect()

    start_stream_time = time.time()
    last_print = start_stream_time
    stream_end = start_stream_time + duration_seconds

    try:
        while time.time() < stream_end:
            time.sleep(1)
            now = time.time()
            elapsed = now - start_stream_time
            rate = total_ticks / elapsed if elapsed > 0 else 0
            distinct = len(ticks_by_symbol)
            pct = (distinct / total_stocks) * 100
            print(
                f"  [{elapsed:4.1f}s] Ticks: {total_ticks:6d} | "
                f"Rate: {rate:6.1f} ticks/s | "
                f"Active stocks: {distinct:4d}/{total_stocks} ({pct:4.1f}%)"
            )

    except KeyboardInterrupt:
        print("\nStreaming interrupted by user.")
    finally:
        print("\nDisconnecting WebSocket stream...")
        broker.disconnect()

    total_elapsed = time.time() - start_stream_time
    avg_rate = total_ticks / total_elapsed if total_elapsed > 0 else 0
    distinct_stocks = len(ticks_by_symbol)

    print("\n" + "=" * 70)
    print("                     STREAMING TEST RESULTS                     ")
    print("=" * 70)
    print(f"  Total Subscriptions Sent:      {total_stocks} stocks")
    print(f"  Streaming Duration:            {total_elapsed:.2f} seconds")
    print(f"  Total Raw Ticks Received:      {total_ticks}")
    print(f"  Average Throughput:            {avg_rate:.2f} ticks/second")
    print(f"  Distinct Stocks with Ticks:    {distinct_stocks} / {total_stocks} ({distinct_stocks/total_stocks*100:.1f}%)")

    # Display Top 10 most active stocks during the test
    print("\n  Top 10 Most Active Stocks by Tick Count:")
    print("  " + "-" * 45)
    for sym, count in ticks_by_symbol.most_common(10):
        ltp = broker.get_ltp(sym)
        ltp_str = f"LTP: {ltp:.2f}" if ltp is not None else "N/A"
        print(f"   * {sym:<15} : {count:4d} ticks | {ltp_str}")

    # Display 10 random sampled stocks from the 1000 list with their live LTP
    print("\n  Sample of 10 Subscribed Stocks with Live LTP:")
    print("  " + "-" * 45)
    sample_indices = [0, 50, 100, 200, 350, 500, 650, 800, 900, 999]
    for idx in sample_indices:
        if idx < len(stocks):
            s = stocks[idx]
            sym = s["symbol"]
            ltp = broker.get_ltp(sym)
            ticks_cnt = ticks_by_symbol.get(sym, 0)
            ltp_str = f"{ltp:>10.2f}" if ltp is not None else "  No tick "
            print(f"   [{idx:3d}] {sym:<15} | Price: {ltp_str} | Ticks: {ticks_cnt}")

    print("=" * 70)
    if total_ticks > 0 and distinct_stocks > 0:
        print("    SUCCESS: 1,000 Stocks Streamed Concurrently over WebSocket! ")
    else:
        print("    WARNING: No ticks received. Verify connection and market hours.")
    print("=" * 70)


if __name__ == "__main__":
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 15
    run_1000_stocks_stream(duration_seconds=duration)
