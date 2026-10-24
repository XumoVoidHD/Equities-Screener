"""Universe Daily Historical Data Preloader.

Fetches 35-day historical daily candles (OHLCV) for up to 1,000 NSE stocks from Dhan REST API
and stores them in `data/dhan/research_sample_daily.json` so the Streamlit dashboard and screener
can evaluate hundreds to thousands of stocks with sub-second response times.

Usage:
    python scripts/cache_universe_daily.py 500     # Cache 500 stocks
    python scripts/cache_universe_daily.py 1000    # Cache 1,000 stocks
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to sys.path so broker can be imported
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from broker import DhanBroker

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def main():
    target_count = int(sys.argv[1]) if len(sys.argv) > 1 else 500

    cache_path = Path("data/dhan/research_sample_daily.json")
    existing_cache = {}
    if cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                existing_cache = json.load(f)
        except Exception:
            existing_cache = {}

    print(f"Existing cache has {len(existing_cache)} stocks.")

    # Load symbol universe
    universe_files = [
        Path("data/dhan/nse_1000_stocks.json"),
        Path("data/dhan/nse_equity_stocks.json"),
    ]

    symbols_to_fetch = []
    for ufile in universe_files:
        if ufile.exists():
            with open(ufile, "r", encoding="utf-8") as f:
                data = json.load(f)
                stocks = data.get("stocks", data) if isinstance(data, dict) else data
                for s in stocks:
                    sym = s.get("symbol")
                    if sym and sym not in existing_cache and sym not in symbols_to_fetch:
                        symbols_to_fetch.append(sym)

    print(f"Found {len(symbols_to_fetch)} new symbols to query.")
    needed = max(0, target_count - len(existing_cache))
    symbols_to_fetch = symbols_to_fetch[:needed]

    if not symbols_to_fetch:
        print(f"Target of {target_count} stocks already met or exceeded in cache ({len(existing_cache)} present)!")
        return

    print(f"Fetching daily candles for {len(symbols_to_fetch)} symbols to reach target {target_count}...")
    broker = DhanBroker()

    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=45)
    from_str = from_dt.strftime("%Y-%m-%d")
    to_str = to_dt.strftime("%Y-%m-%d")

    success_count = 0
    t0 = time.time()

    for i, sym in enumerate(symbols_to_fetch):
        try:
            res = broker.get_historical_daily_bars(sym, from_str, to_str)
            candles = res.get("data", {}) if isinstance(res, dict) else {}
            if candles.get("close") and len(candles.get("close")) >= 15:
                existing_cache[sym] = candles
                success_count += 1
        except Exception as e:
            pass

        # Dhan REST API rate pacing (80ms between requests)
        time.sleep(0.08)

        if (i + 1) % 25 == 0 or (i + 1) == len(symbols_to_fetch):
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  [{i+1}/{len(symbols_to_fetch)}] Success: {success_count} | Total in cache: {len(existing_cache)} | Speed: {rate:.1f} req/s")

            # Periodic save
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(existing_cache, f)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(existing_cache, f)

    print("\n" + "=" * 60)
    print(f"Cache update complete! Total stocks in universe: {len(existing_cache)}")
    print(f"Saved to: {cache_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
