"""Pure Indian Equity Stocks Streaming Benchmark.

Streams actual common stocks / equities only (excluding bonds, debentures,
mutual funds, ETFs, SME, and trade-to-trade surveillance shares):
- Mode 'nse':    All 2,736 pure NSE Equities (Series EQ)
- Mode 'unique': All 2,884 unique Indian stock companies (2,736 NSE + 148 BSE exclusive) [Default]
- Mode 'all':    All 5,347 pure equity listings across NSE (EQ) & BSE (Groups A & B)

Usage:
    python test_equities_stream.py [mode: unique|nse|all] [duration_seconds]
Examples:
    python test_equities_stream.py unique 15
    python test_equities_stream.py nse 15
    python test_equities_stream.py all 15
"""

import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from broker import DhanBroker, ExchangeSegment

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def stream_equities(mode: str = "unique", duration_seconds: int = 15):
    mode = mode.lower().strip()
    if mode == "nse":
        data_file = Path("data/dhan/nse_equity_stocks.json")
        desc = "Pure NSE Equities (Series EQ)"
    elif mode == "all":
        data_file = Path("data/dhan/all_pure_equity_stocks.json")
        desc = "All Pure Equities Across NSE & BSE"
    else:
        mode = "unique"
        data_file = Path("data/dhan/unique_indian_stocks.json")
        desc = "Unique Indian Stock Companies (NSE Primary + BSE Exclusive)"

    if not data_file.exists():
        print(f"Error: Dataset {data_file} not found.")
        sys.exit(1)

    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    stocks = data["stocks"]
    total_count = len(stocks)
    nse_count = sum(1 for s in stocks if s.get("exchange") == "NSE")
    bse_count = sum(1 for s in stocks if s.get("exchange") == "BSE")

    print("=" * 75)
    print(f"     STREAMING PURE INDIAN EQUITIES ({desc.upper()})     ")
    print("=" * 75)
    print(f"\n[1/4] Loaded Pure Equities Dataset ({mode}):")
    print(f"      - Total Stocks:  {total_count:,}")
    print(f"      - NSE Equities:  {nse_count:,}")
    print(f"      - BSE Equities:  {bse_count:,}")

    # Exchange lookup mapping: (exchange_segment, security_id) -> (exchange, symbol)
    inst_lookup = {
        (s["exchange_segment"], str(s["security_id"])): (s["exchange"], s["symbol"])
        for s in stocks
    }

    # 2. Initialize Broker
    print("\n[2/4] Initializing DhanBroker...")
    broker = DhanBroker()
    print(f"      Client ID: {broker.config.client_id}")

    # Tick and coverage metrics
    total_ticks = 0
    ticks_by_symbol = Counter()
    active_symbols = set()
    nse_ticks = 0
    bse_ticks = 0

    def on_tick(tick):
        nonlocal total_ticks, nse_ticks, bse_ticks
        total_ticks += 1
        ticks_by_symbol[tick.symbol] += 1
        active_symbols.add(tick.symbol)
        if tick.exchange_segment == 1:
            nse_ticks += 1
        else:
            bse_ticks += 1

    broker.on_tick(on_tick)

    # 3. Subscribe
    print(f"\n[3/4] Subscribing to all {total_count:,} equity stocks...")
    sub_tuples = [(s["exchange_segment"], s["security_id"]) for s in stocks]
    t_start = time.time()
    broker.subscribe_ltp(sub_tuples)
    print(f"      Mapped {len(sub_tuples):,} subscriptions in {time.time() - t_start:.3f}s")

    # 4. Connect & Stream
    print(f"\n[4/4] Connecting WebSocket and streaming for {duration_seconds} seconds...\n")
    broker.connect()

    start_stream = time.time()
    stream_end = start_stream + duration_seconds

    try:
        while time.time() < stream_end:
            time.sleep(1)
            now = time.time()
            elapsed = now - start_stream
            rate = total_ticks / elapsed if elapsed > 0 else 0
            cov = len(active_symbols)
            pct = (cov / total_count) * 100 if total_count else 0
            print(
                f"  [{elapsed:4.1f}s] Ticks: {total_ticks:6d} | "
                f"Rate: {rate:6.1f} t/s | "
                f"Active Equities: {cov:4d}/{total_count:,} ({pct:4.1f}%)"
            )

    except KeyboardInterrupt:
        print("\nStreaming interrupted by user.")
    finally:
        print("\nDisconnecting WebSocket stream...")
        broker.disconnect()

    # Final summary report
    elapsed_total = time.time() - start_stream
    avg_rate = total_ticks / elapsed_total if elapsed_total > 0 else 0
    active_total = len(active_symbols)
    cov_pct = (active_total / total_count) * 100 if total_count else 0

    print("\n" + "=" * 75)
    print("                    PURE EQUITIES STREAM REPORT                          ")
    print("=" * 75)
    print(f"  Mode:                         {desc}")
    print(f"  Total Equities Subscribed:    {total_count:,} stocks")
    print(f"  Streaming Duration:           {elapsed_total:.2f} seconds")
    print(f"  Total Ticks Received:         {total_ticks:,}")
    print(f"  Average Throughput:           {avg_rate:,.1f} ticks/second")
    print(f"  Active Equities Covered:      {active_total:,} / {total_count:,} ({cov_pct:.1f}%)")
    print(f"  - NSE Equity Ticks:           {nse_ticks:,}")
    print(f"  - BSE Equity Ticks:           {bse_ticks:,}")

    # Top 10 Active Stocks
    print("\n  Top 10 Most Active Equities by Tick Count:")
    print("  " + "-" * 55)
    for sym, count in ticks_by_symbol.most_common(10):
        ltp = broker.get_ltp(sym)
        ltp_str = f"Rs. {ltp:>10.2f}" if ltp is not None else "     N/A  "
        print(f"   * {sym:<16} : {count:4d} ticks | Price: {ltp_str}")

    # Sample Blue-chips, Mid-caps, and Small-caps
    print("\n  Live Price Samples of Major Equity Stocks:")
    print("  " + "-" * 55)
    sample_benchmarks = [
        ("RELIANCE", "Reliance Industries"),
        ("TCS", "Tata Consultancy Services"),
        ("HDFCBANK", "HDFC Bank Ltd"),
        ("INFY", "Infosys Ltd"),
        ("SBIN", "State Bank of India"),
        ("TATAMOTORS", "Tata Motors Ltd"),
        ("ITC", "ITC Ltd"),
        ("BHARTIARTL", "Bharti Airtel Ltd"),
        ("MARUTI", "Maruti Suzuki Ltd"),
        ("LT", "Larsen & Toubro Ltd"),
    ]
    for sym, name in sample_benchmarks:
        ltp = broker.get_ltp(sym)
        ltp_str = f"Rs. {ltp:>10.2f}" if ltp is not None else "     N/A  "
        print(f"   {sym:<14} ({name:<28}): {ltp_str}")

    print("=" * 75)
    if total_ticks > 100:
        print("   SUCCESS: Pure Equity Stocks Streamed and Processed Live!     ")
    print("=" * 75)


if __name__ == "__main__":
    sel_mode = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].isdigit() else "unique"
    dur = 15
    for arg in sys.argv[1:]:
        if arg.isdigit():
            dur = int(arg)
            break
    stream_equities(mode=sel_mode, duration_seconds=dur)
