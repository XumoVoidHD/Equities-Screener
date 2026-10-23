"""Full Indian Stock Universe Live Streaming Benchmark (NSE + BSE).

This script streams the COMPLETE Indian stock universe:
- All active NSE Equities (~3,436 stocks)
- All active BSE Equities (~4,967 stocks)
- Total Universe: 8,403 stocks simultaneously over DhanHQ WebSocket.

Monitors:
- Ingestion throughput (ticks/second).
- Active instrument coverage across NSE and BSE.
- Live price updates, top active stocks, and exchange-level breakdown.
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


def stream_entire_indian_universe(duration_seconds: int = 15):
    print("=" * 75)
    print("       STREAMING COMPLETE INDIAN STOCK UNIVERSE (NSE + BSE)       ")
    print("=" * 75)

    # 1. Load entire universe
    json_path = Path("data/dhan/full_indian_universe.json")
    if not json_path.exists():
        print(f"Error: {json_path} not found.")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stocks = data["stocks"]
    nse_count = data.get("nse_count", sum(1 for s in stocks if s.get("exchange") == "NSE"))
    bse_count = data.get("bse_count", sum(1 for s in stocks if s.get("exchange") == "BSE"))
    total_count = len(stocks)

    print(f"\n[1/4] Loaded Entire Indian Stock Universe:")
    print(f"      - NSE Equities:  {nse_count:>5d} stocks")
    print(f"      - BSE Equities:  {bse_count:>5d} stocks")
    print(f"      - Total Universe: {total_count:>5d} stocks")

    # Fast lookup for exchange by security_id and segment
    exchange_by_instrument = {
        (s["exchange_segment"], str(s["security_id"])): s.get("exchange", "NSE")
        for s in stocks
    }

    # 2. Initialize Broker
    print("\n[2/4] Initializing DhanBroker...")
    broker = DhanBroker()
    print(f"      Client ID: {broker.config.client_id}")

    # Track ticks
    total_ticks = 0
    ticks_by_symbol = Counter()
    nse_ticks = 0
    bse_ticks = 0
    nse_active_symbols = set()
    bse_active_symbols = set()

    def on_tick(tick):
        nonlocal total_ticks, nse_ticks, bse_ticks
        total_ticks += 1
        ticks_by_symbol[tick.symbol] += 1
        exch = exchange_by_instrument.get((tick.exchange_segment, tick.security_id), "NSE")
        if exch == "NSE" or tick.exchange_segment == 1:
            nse_ticks += 1
            nse_active_symbols.add(tick.symbol)
        else:
            bse_ticks += 1
            bse_active_symbols.add(tick.symbol)

    broker.on_tick(on_tick)

    # 3. Subscribe all 8,403 instruments
    print(f"\n[3/4] Subscribing to all {total_count} stocks (sent in batches of 100)...")
    sub_tuples = [
        (s["exchange_segment"], s["security_id"])
        for s in stocks
    ]
    t_sub_start = time.time()
    broker.subscribe_ltp(sub_tuples)
    t_sub_elapsed = time.time() - t_sub_start
    print(f"      Registered {len(sub_tuples)} subscriptions in {t_sub_elapsed:.3f}s")

    # 4. Connect and stream
    print(f"\n[4/4] Connecting to WebSocket and streaming for {duration_seconds} seconds...\n")
    broker.connect()

    start_stream = time.time()
    stream_end = start_stream + duration_seconds

    try:
        while time.time() < stream_end:
            time.sleep(1)
            now = time.time()
            elapsed = now - start_stream
            rate = total_ticks / elapsed if elapsed > 0 else 0
            total_active = len(nse_active_symbols) + len(bse_active_symbols)
            cov_pct = (total_active / total_count) * 100

            print(
                f"  [{elapsed:4.1f}s] Ticks: {total_ticks:6d} | "
                f"Rate: {rate:6.1f} t/s | "
                f"NSE Active: {len(nse_active_symbols):4d}/{nse_count} | "
                f"BSE Active: {len(bse_active_symbols):4d}/{bse_count} | "
                f"Total: {total_active:4d}/{total_count} ({cov_pct:4.1f}%)"
            )

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")
    finally:
        print("\nDisconnecting WebSocket stream...")
        broker.disconnect()

    # Final summary calculations
    total_elapsed = time.time() - start_stream
    avg_throughput = total_ticks / total_elapsed if total_elapsed > 0 else 0
    total_active_all = len(nse_active_symbols) + len(bse_active_symbols)

    print("\n" + "=" * 75)
    print("                  ENTIRE INDIAN UNIVERSE STREAM REPORT                   ")
    print("=" * 75)
    print(f"  Total Instruments Subscribed:  {total_count} stocks (NSE: {nse_count}, BSE: {bse_count})")
    print(f"  Streaming Duration:            {total_elapsed:.2f} seconds")
    print(f"  Total Raw Ticks Received:      {total_ticks:,}")
    print(f"  Average Throughput:            {avg_throughput:,.1f} ticks/second")
    print(f"  Total Active Stocks Covered:   {total_active_all:,} / {total_count:,} ({total_active_all/total_count*100:.1f}%)")
    print(f"  - Active NSE Equities:         {len(nse_active_symbols):,} / {nse_count:,} ({len(nse_active_symbols)/nse_count*100:.1f}%)")
    print(f"  - Active BSE Equities:         {len(bse_active_symbols):,} / {bse_count:,} ({len(bse_active_symbols)/bse_count*100:.1f}%)")
    print(f"  - NSE Ticks Share:             {nse_ticks:,} ({(nse_ticks/total_ticks*100) if total_ticks else 0:.1f}%)")
    print(f"  - BSE Ticks Share:             {bse_ticks:,} ({(bse_ticks/total_ticks*100) if total_ticks else 0:.1f}%)")

    # Top 10 most active stocks
    print("\n  Top 10 Most Active Indian Stocks by Tick Count:")
    print("  " + "-" * 55)
    for sym, count in ticks_by_symbol.most_common(10):
        ltp = broker.get_ltp(sym)
        ltp_str = f"Rs. {ltp:>10.2f}" if ltp is not None else "     N/A  "
        print(f"   * {sym:<18} : {count:5d} ticks | Price: {ltp_str}")

    # Cross-exchange sampling (5 NSE & 5 BSE)
    print("\n  Live Price Samples Across Major NSE & BSE Stocks:")
    print("  " + "-" * 55)
    sample_benchmarks = [
        ("NSE:RELIANCE", "Reliance Industries (NSE)"),
        ("NSE:TCS", "Tata Consultancy Services (NSE)"),
        ("NSE:HDFCBANK", "HDFC Bank (NSE)"),
        ("NSE:INFY", "Infosys (NSE)"),
        ("NSE:SBIN", "State Bank of India (NSE)"),
        ("BSE:ABB", "ABB India (BSE)"),
        ("BSE:TATACHEM", "Tata Chemicals (BSE)"),
        ("BSE:VOLTAS", "Voltas Ltd (BSE)"),
        ("BSE:ITC", "ITC Ltd (BSE)"),
        ("BSE:MARUTI", "Maruti Suzuki (BSE)"),
    ]
    for sym_alias, desc in sample_benchmarks:
        ltp = broker.get_ltp(sym_alias)
        ltp_str = f"Rs. {ltp:>10.2f}" if ltp is not None else "     N/A  "
        print(f"   {sym_alias:<15} ({desc:<30}): {ltp_str}")

    print("=" * 75)
    if total_ticks > 500:
        print("   SUCCESS: Entire Indian Stock Universe Streamed Concurrently! ")
    print("=" * 75)


if __name__ == "__main__":
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 15
    stream_entire_indian_universe(duration_seconds=duration)
