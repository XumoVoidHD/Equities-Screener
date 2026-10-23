"""Example: Streaming LTP and Candlestick Bars using the DhanBroker wrapper.

This script demonstrates:
1. Connecting to DhanHQ live WebSocket feed.
2. Streaming Last Traded Price (LTP) with per-symbol callbacks.
3. Streaming 1-minute and 5-second candlestick bars.
4. Using non-blocking background streaming and context managers.
"""

import time
from broker import DhanBroker, Bar, Tick, TimeFrame


def main():
    print("Initializing DhanBroker...")
    # Credentials are automatically read from .env (or pass env_path='.env')
    broker = DhanBroker()

    # 1. Register global tick callback (optional)
    def on_tick(tick: Tick):
        # Raw tick with exchange segment, security ID, price, volume, timestamp
        pass
    broker.on_tick(on_tick)

    # 2. Subscribe to real-time LTP for specific symbols
    def on_nifty_ltp(symbol: str, ltp: float, ts):
        print(f"[LTP] {symbol} => {ltp:.2f} at {ts.strftime('%H:%M:%S')}")

    def on_reliance_ltp(symbol: str, ltp: float, ts):
        print(f"[LTP] {symbol} => {ltp:.2f} at {ts.strftime('%H:%M:%S')}")

    broker.subscribe_ltp("NIFTY", callback=on_nifty_ltp)
    broker.subscribe_ltp("RELIANCE", callback=on_reliance_ltp)

    # 3. Subscribe to real-time Bar updates & Bar closes (e.g., 1-minute or 5-second candles)
    def on_bar_completed(bar: Bar):
        print(f"\n[BAR CLOSED] {bar.symbol} [{bar.timeframe}] O={bar.open:.2f} H={bar.high:.2f} L={bar.low:.2f} C={bar.close:.2f} Vol={bar.volume}\n")

    def on_bar_tick(bar: Bar):
        # Called on each tick updating the current open bar
        pass

    broker.subscribe_bars(
        symbols=["NIFTY", "RELIANCE"],
        timeframe=TimeFrame.MINUTE_1,
        on_bar_close=on_bar_completed,
        on_bar_update=on_bar_tick,
    )

    # 4. Start streaming in background thread
    print("Starting stream (press Ctrl+C to stop)...")
    broker.connect()

    try:
        # Run loop or screener logic here
        for _ in range(15):
            time.sleep(1)
            # You can query cached LTP and in-progress bars at any time:
            nifty_price = broker.get_ltp("NIFTY")
            current_bar = broker.get_latest_bar("NIFTY", TimeFrame.MINUTE_1)
            if current_bar:
                print(f"Current NIFTY 1m Bar: O={current_bar.open:.2f} H={current_bar.high:.2f} L={current_bar.low:.2f} C={current_bar.close:.2f}")

    except KeyboardInterrupt:
        print("\nStopping stream...")
    finally:
        broker.disconnect()
        print("Disconnected cleanly.")


if __name__ == "__main__":
    main()
