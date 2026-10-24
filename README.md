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

---

### 5. NautilusTrader Real-Time Equity Screener Actor

A production-grade `Actor` inheriting from `nautilus_trader.common.actor.Actor` that monitors all subscribed stocks in real-time and evaluates screening criteria (volume surge, price momentum, range expansion, and Donchian breakouts):

```python
from broker import DhanBroker, NautilusEquityScreenerActor, ScreenerConfig

# 1. Configure detection criteria
config = ScreenerConfig(
    lookback_bars=10,
    min_volume_ratio=2.0,      # Volume surge >= 2x average volume
    min_price_change_pct=1.0,  # Price momentum >= 1.0%
    check_breakout=True,       # Detect new N-period highs/lows
)

# 2. Instantiate Nautilus Actor
screener = NautilusEquityScreenerActor(config=config)

def on_match(match):
    print(f"ALERT: {match.symbol} Rs.{match.price:.2f} [{', '.join(match.reasons)}]")

screener.on_match(on_match)

# 3. Attach directly to DhanBroker live feed
broker = DhanBroker(bar_timeframes=["1m"])
screener.attach_to_broker(broker)

# 4. Subscribe to any equity universe (or all 2,884 pure equities)
broker.subscribe_bars(["RELIANCE", "TCS", "INFY", "TATAMOTORS"], timeframe="1m")
broker.connect()
```

#### Run the Nautilus Screener Test
```bash
# Synthetic bar verification + live bridge test
python test_nautilus_screener.py live 10
```

---

### 6. Configurable Multi-Condition Screener (Chartink-Style)

The screener includes a fully configurable condition evaluation engine implementing 5 momentum, price breakout, and RSI surge conditions:

| # | Condition Name | Default Expression | Configurable Parameters |
|---|---|---|---|
| **1** | **Hourly Close > Daily Open** | `Hourly Close > Daily Open` | `factor` (default `1.0`), `offset` (default `0.0`), `operator` (`>`, `>=`, etc.), `enabled` |
| **2** | **Hourly Close > Discounted Prev Close** | `Hourly Close > ([-1] Daily Close * 0.95)` | `lookback_days` (default `1`), `factor` (default `0.95`), `operator`, `enabled` |
| **3** | **Hourly Close >= Past Daily Close** | `Hourly Close >= [-10] Daily Close` | `lookback_days` (default `10`), `factor` (default `1.0`), `operator`, `enabled` |
| **4** | **Daily RSI Surge** | `1day RSI(14) > ([-1] 1day RSI(14) + 10)` | `rsi_window` (default `14`), `lookback_days` (default `1`), `rsi_delta` (default `10.0`), `enabled` |
| **5** | **Daily Volume Threshold** | `Daily Volume > 5,000,000` | `min_volume` (default `5,000,000`), `enabled` (*disabled by default*) |

#### A. Configure via JSON (`config/screener_conditions.json`)
All conditions and their parameters can be customized without editing code:
```json
{
  "screener_name": "Chartink Intraday Momentum & RSI Surge",
  "logic": "AND",
  "conditions": {
    "condition_1": { "enabled": true, "operator": ">", "factor": 1.0, "offset": 0.0 },
    "condition_2": { "enabled": true, "operator": ">", "lookback_days": 1, "factor": 0.95 },
    "condition_3": { "enabled": true, "operator": ">=", "lookback_days": 10, "factor": 1.0 },
    "condition_4": { "enabled": true, "operator": ">", "rsi_window": 14, "lookback_days": 1, "rsi_delta": 10.0 },
    "condition_5": { "enabled": false, "operator": ">", "min_volume": 5000000.0 }
  }
}
```

#### B. Dynamic Configuration in Python
```python
from broker import NautilusEquityScreenerActor, ScreenerConditionsConfig

# 1. Load from JSON or create programmatically
screener = NautilusEquityScreenerActor(conditions_json_path="config/screener_conditions.json")

# 2. Modify parameters dynamically on the fly
screener.set_condition_param("condition_2", "factor", 0.92)      # 8% discount tolerance
screener.set_condition_param("condition_4", "rsi_delta", 8.0)     # Require +8 RSI jump instead of +10
screener.enable_condition("condition_5", True)                   # Enable volume condition
screener.set_condition_param("condition_5", "min_volume", 2000000.0)

# 3. Save modified rules
screener.save_conditions("config/my_custom_conditions.json")
```

#### C. Run the Condition Verification Tests
```bash
# 1. Run unit checks, dynamic reconfiguration, and synthetic bar pipeline tests:
python test_chartink_screener.py

# 2. Run with real Dhan REST API historical daily data for sample equities:
python test_chartink_screener.py real
```


