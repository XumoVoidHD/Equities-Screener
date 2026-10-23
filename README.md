# DhanHQ Market Data Broker Wrapper

A high-performance Python broker wrapper around the **DhanHQ SDK** specifically engineered to stream real-time **LTP (Last Traded Price)** and **OHLCV Candlestick Bars**.

---

## ✨ Features

- **Automated Authentication & Token Lifecycle**:
  - Automatically loads credentials from `.env` (`DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP_SECRET`).
  - Generates TOTP dynamically using `pyotp`.
  - Caches 24-hour access tokens in `data/dhan/token.json` with active profile validation to respect Dhan's 2-minute token regeneration limit.
- **Real-Time Streaming**:
  - Low-latency binary WebSocket streaming via DhanHQ `MarketFeed v2`.
  - Stream tick-by-tick **LTP** with global and per-symbol callbacks.
  - Stream real-time **Candlestick Bars (OHLCV)** across custom intervals (`1s`, `5s`, `1m`, `3m`, `5m`, `15m`, `1h`, etc.).
  - Emits `on_bar_close` when a candle finishes and `on_bar_update` on every price movement.
- **Convenient Symbol Resolution**:
  - Supports trading symbols (`"NIFTY"`, `"RELIANCE"`, `"TCS"`, `"HDFCBANK"`), segment prefixes (`"NSE:RELIANCE"`), and explicit tuples `(ExchangeSegment.NSE, "2885")`.
- **Cached Instant Queries**:
  - `broker.get_ltp("NIFTY")` -> instant cached price.
  - `broker.get_latest_bar("NIFTY", "1m")` -> currently open candle.
  - `broker.get_bars("NIFTY", "1m")` -> recent completed candles.
- **Historical Data**:
  - Fetch historical 1-minute bars via Dhan REST API with `broker.get_historical_minute_bars(...)`.

---

## 📁 Architecture

```
aryaman_screener/
├── broker/
│   ├── __init__.py           # Exports: DhanBroker, Tick, Bar, ExchangeSegment, FeedMode, TimeFrame
│   ├── client.py             # Main DhanBroker wrapper
│   ├── auth.py               # DhanAuthManager (TOTP minting, caching, token validation)
│   ├── config.py             # Config loader from .env
│   ├── models.py             # Data classes (Tick, Bar, Enums)
│   ├── bar_aggregator.py     # Real-time tick-to-bar aggregator & candle closer
│   └── scrip_master.py       # Trading symbol & security ID resolver
├── data/
│   └── dhan/
│       └── token.json        # Cached access token
├── test_broker.py            # Live streaming verification test
└── examples/
    └── live_stream_example.py# Example usage
```

---

## 🚀 Quickstart

### 1. Configure `.env`
Ensure your `.env` contains:
```env
DHAN_CLIENT_ID=your_client_id
DHAN_PIN=your_pin
DHAN_TOTP_SECRET=your_totp_secret
```

### 2. Stream LTP
```python
from broker import DhanBroker

broker = DhanBroker()

# Subscribe to LTP with a callback
def on_ltp(symbol, ltp, ts):
    print(f"{symbol} LTP: {ltp:.2f} at {ts.strftime('%H:%M:%S')}")

broker.subscribe_ltp("RELIANCE", callback=on_ltp)
broker.connect()
```

### 3. Stream Bars (Candlesticks)
```python
from broker import DhanBroker, TimeFrame

broker = DhanBroker()

def on_candle_close(bar):
    print(f"Candle Closed: {bar.symbol} [{bar.timeframe}] O:{bar.open} H:{bar.high} L:{bar.low} C:{bar.close} Vol:{bar.volume}")

broker.subscribe_bars(
    symbols=["NIFTY", "RELIANCE"],
    timeframe=TimeFrame.MINUTE_1,   # or "5s", "5m", "15m"
    on_bar_close=on_candle_close
)

broker.connect()
```

### 4. Run the Verification Tests

#### A. Basic Verification (NIFTY & RELIANCE LTP + 5s/1m Bars)
```bash
python test_broker.py
```

#### B. 1,000 Stocks Stream Benchmark
Streams the top 1,000 active NSE equities simultaneously:
```bash
python test_1000_stocks_stream.py 15
```

#### C. Pure Indian Equity Stocks Stream (Just Stocks, No Bonds/ETFs/Debt)
Streams real common equity stocks only:
```bash
# 1. Stream all 2,884 unique Indian stock companies (NSE primary + BSE exclusive):
python test_equities_stream.py unique 15

# 2. Stream all 2,736 pure NSE equities (Series EQ):
python test_equities_stream.py nse 15

# 3. Stream all 5,347 pure equities across NSE (EQ) & BSE (Groups A & B):
python test_equities_stream.py all 15
```

#### D. Full Market Universe (All 8,403 Instruments across NSE & BSE)
```bash
python test_full_universe_stream.py 15
```

