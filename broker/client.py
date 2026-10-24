from __future__ import annotations

import logging
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import dhanhq
from broker.auth import DhanAuthManager
from broker.bar_aggregator import BarAggregator
from broker.config import DhanConfig
from broker.models import Bar, ExchangeSegment, FeedMode, Tick, TimeFrame
from broker.scrip_master import ScripMaster
from broker.stream_engine import WebSocketStreamEngine

logger = logging.getLogger("dhan.broker")


class DhanBroker:
    """Production-grade broker wrapper around DhanHQ SDK for streaming LTP and Bars.

    Features:
        - Automatic authentication via TOTP & PIN with 24h token caching.
        - High-performance WebSocket streaming via DhanHQ MarketFeed v2.
        - Stream Last Traded Price (LTP) with per-symbol or global callbacks.
        - Stream real-time Candlestick Bars (OHLCV) across custom timeframes ('1s', '5s', '1m', '5m', etc.).
        - In-memory cached latest LTP and Bar history.
        - Automatic symbol resolution (e.g. 'NIFTY', 'RELIANCE', 'NSE:TCS').
        - Supports both non-blocking background streaming and blocking run_forever.
        - Context manager support (`with DhanBroker() as broker:`).
    """

    def __init__(
        self,
        config: DhanConfig | None = None,
        env_path: str | None = None,
        bar_timeframes: list[str] | None = None,
        auto_connect: bool = False,
    ):
        self.config = config or DhanConfig.from_env(env_path)
        self.auth_manager = DhanAuthManager(self.config)
        self.scrip_master = ScripMaster()
        self.bar_aggregator = BarAggregator(
            timeframes=bar_timeframes or [TimeFrame.MINUTE_1],
            on_bar_close=self._dispatch_bar_close,
            on_bar_update=self._dispatch_bar_update,
        )

        # Internal state
        self._lock = threading.RLock()
        self._engine: WebSocketStreamEngine | None = None
        self._feed_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None
        self._tick_queue: queue.Queue = queue.Queue(maxsize=200000)
        self._is_running = False
        self._is_connected = False

        # Subscriptions: (exchange_segment, security_id) -> FeedMode
        self._subscriptions: dict[tuple[int, str], FeedMode] = {}
        # Reverse mapping: (exchange_segment, security_id) -> symbol
        self._instrument_to_symbol: dict[tuple[int, str], str] = {}

        # Caches
        self._ltp_cache: dict[str, float] = {}
        self._tick_cache: dict[str, Tick] = {}

        # Callbacks
        self._tick_callbacks: list[Callable[[Tick], None]] = []
        self._global_ltp_callbacks: list[Callable[[str, float, datetime], None]] = []
        self._symbol_ltp_callbacks: dict[str, list[Callable[[str, float, datetime], None]]] = {}
        self._bar_close_callbacks: list[Callable[[Bar], None]] = []
        self._bar_update_callbacks: list[Callable[[Bar], None]] = []

        if auto_connect:
            self.connect()

    # -------------------------------------------------------------------------
    # Subscription API
    # -------------------------------------------------------------------------

    def subscribe_ltp(
        self,
        symbols: str | tuple | list[str | tuple],
        callback: Callable[[str, float, datetime], None] | None = None,
    ) -> list[tuple[int, str, str]]:
        """Subscribe to real-time LTP (Ticker feed) for one or more symbols.

        Args:
            symbols: Symbol string (e.g. 'RELIANCE', 'NIFTY'), tuple (segment, sec_id),
                     or a list of such symbols.
            callback: Optional callback `cb(symbol, ltp, timestamp)`.

        Returns:
            List of resolved `(exchange_segment, security_id, symbol)` tuples.
        """
        symbol_list = [symbols] if isinstance(symbols, (str, tuple)) else list(symbols)
        resolved_items = [self.scrip_master.resolve(s) for s in symbol_list]

        with self._lock:
            for seg, sec_id, sym in resolved_items:
                key = (seg, sec_id)
                self._subscriptions[key] = FeedMode.TICKER
                self._instrument_to_symbol[key] = sym
                if callback:
                    self._symbol_ltp_callbacks.setdefault(sym, []).append(callback)
                    self._symbol_ltp_callbacks.setdefault(sec_id, []).append(callback)

            # If WebSocket feed is active, send dynamic subscription update
            if self._engine and self._is_connected:
                tuples_to_sub = [(seg, sec_id, FeedMode.TICKER.value) for seg, sec_id, _ in resolved_items]
                self._engine.subscribe_dynamic(tuples_to_sub)

        return resolved_items

    def subscribe_bars(
        self,
        symbols: str | tuple | list[str | tuple],
        timeframe: str = TimeFrame.MINUTE_1,
        on_bar_close: Callable[[Bar], None] | None = None,
        on_bar_update: Callable[[Bar], None] | None = None,
    ) -> list[tuple[int, str, str]]:
        """Subscribe to real-time Bar (candlestick) generation for symbols.

        Args:
            symbols: Symbol string, tuple, or list of symbols.
            timeframe: Bar interval, e.g. '1s', '5s', '1m', '5m', '15m'.
            on_bar_close: Callback invoked when a bar timeframe finishes.
            on_bar_update: Callback invoked on every tick that updates open bar.

        Returns:
            List of resolved `(exchange_segment, security_id, symbol)` tuples.
        """
        self.bar_aggregator.add_timeframe(timeframe)
        if on_bar_close:
            self._bar_close_callbacks.append(on_bar_close)
        if on_bar_update:
            self._bar_update_callbacks.append(on_bar_update)

        # Bar generation requires at least LTP/Ticker or Quote ticks
        symbol_list = [symbols] if isinstance(symbols, (str, tuple)) else list(symbols)
        resolved_items = [self.scrip_master.resolve(s) for s in symbol_list]

        with self._lock:
            tuples_to_sub = []
            for seg, sec_id, sym in resolved_items:
                key = (seg, sec_id)
                # Indices (IDX) only support TICKER mode, equities support QUOTE
                mode = FeedMode.TICKER if seg == ExchangeSegment.IDX.value else FeedMode.QUOTE
                self._subscriptions[key] = mode
                self._instrument_to_symbol[key] = sym
                tuples_to_sub.append((seg, sec_id, mode.value))

            if self._engine and self._is_connected:
                self._engine.subscribe_dynamic(tuples_to_sub)

        return resolved_items

    def unsubscribe(self, symbols: str | tuple | list[str | tuple]) -> None:
        """Unsubscribe from real-time data for symbols."""
        symbol_list = [symbols] if isinstance(symbols, (str, tuple)) else list(symbols)
        resolved_items = [self.scrip_master.resolve(s) for s in symbol_list]

        with self._lock:
            to_unsub_tuples = []
            for seg, sec_id, _ in resolved_items:
                key = (seg, sec_id)
                mode = self._subscriptions.pop(key, FeedMode.TICKER)
                self._instrument_to_symbol.pop(key, None)
                to_unsub_tuples.append((seg, sec_id, mode.value))

            if self._engine and self._is_connected and to_unsub_tuples:
                self._engine.unsubscribe_dynamic(to_unsub_tuples)

    # -------------------------------------------------------------------------
    # Callbacks Registration
    # -------------------------------------------------------------------------

    def on_tick(self, callback: Callable[[Tick], None]) -> None:
        """Register a callback called on every received tick."""
        self._tick_callbacks.append(callback)

    def on_ltp(self, callback: Callable[[str, float, datetime], None], symbol: str | None = None) -> None:
        """Register a callback called when LTP changes (globally or for a specific symbol)."""
        if symbol:
            clean = symbol.strip().upper()
            self._symbol_ltp_callbacks.setdefault(clean, []).append(callback)
        else:
            self._global_ltp_callbacks.append(callback)

    def on_bar_close(self, callback: Callable[[Bar], None]) -> None:
        """Register a callback called when any bar finishes/closes."""
        self._bar_close_callbacks.append(callback)

    def on_bar_update(self, callback: Callable[[Bar], None]) -> None:
        """Register a callback called when an open bar is updated with a new tick."""
        self._bar_update_callbacks.append(callback)

    # -------------------------------------------------------------------------
    # Lifecycle & Connection
    # -------------------------------------------------------------------------

    def connect(self) -> None:
        """Initialize and start the live market feed in a background thread."""
        with self._lock:
            if self._is_running:
                logger.warning("DhanBroker is already running.")
                return

            access_token = self.auth_manager.get_access_token()
            instruments = [
                (seg, sec_id, mode.value)
                for (seg, sec_id), mode in self._subscriptions.items()
            ]

            self._engine = WebSocketStreamEngine(
                client_id=self.config.client_id,
                access_token=access_token,
                on_message=self._on_message,
                on_connect=self._on_connect,
                on_close=self._on_close,
                on_error=self._on_error,
            )

            self._is_running = True
            # Start high-throughput worker thread to decouple I/O from callback execution
            self._worker_thread = threading.Thread(
                target=self._worker_loop, daemon=True, name="DhanTickWorkerThread"
            )
            self._worker_thread.start()

            self._feed_thread = self._engine.start(initial_subscriptions=instruments)
            logger.info("DhanBroker background feed started.")

    def run_forever(self) -> None:
        """Start the live market feed synchronously (blocking)."""
        self.connect()
        try:
            while self._is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.disconnect()

    def disconnect(self) -> None:
        """Stop and disconnect the WebSocket feed."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False
            self._is_connected = False
            if self._engine:
                try:
                    self._engine.stop()
                except Exception as e:
                    logger.debug("Error while closing stream engine: %s", e)
                self._engine = None
            if self._tick_queue:
                try:
                    self._tick_queue.put_nowait(None)
                except Exception:
                    pass
            logger.info("DhanBroker disconnected.")

    def __enter__(self) -> DhanBroker:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """Returns True if the WebSocket connection is open and active."""
        return self._is_connected

    # -------------------------------------------------------------------------
    # Internal Feed Callbacks
    # -------------------------------------------------------------------------

    def _on_connect(self) -> None:
        self._is_connected = True
        logger.info("Successfully connected to DhanHQ MarketFeed WebSocket.")

    def _on_close(self) -> None:
        self._is_connected = False
        logger.info("DhanHQ MarketFeed WebSocket closed.")

    def _on_error(self, error: Any) -> None:
        logger.error("DhanHQ MarketFeed error: %s", error)

    def _on_message(self, raw: dict) -> None:
        """Enqueue incoming raw WebSocket packets for worker thread to process."""
        if not isinstance(raw, dict):
            return
        try:
            self._tick_queue.put_nowait(raw)
        except queue.Full:
            pass

    def _worker_loop(self) -> None:
        """Dedicated worker thread processing queued market ticks."""
        q = self._tick_queue
        while self._is_running:
            try:
                raw = q.get(timeout=0.1)
                if raw is None:
                    break
                self._handle_raw_packet(raw)
                q.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("Error in tick worker loop: %s", e)

    def _handle_raw_packet(self, raw: dict) -> None:
        """Process incoming raw WebSocket packets from Dhan."""
        if not isinstance(raw, dict):
            return

        msg_type = raw.get("type", "")

        # Only process packets containing market pricing
        if msg_type not in ("Ticker Data", "Quote Data", "Full Data", "Previous Close"):
            return

        # Extract core fields
        seg = raw.get("exchange_segment")
        sec_id = str(raw.get("security_id", ""))
        ltp_raw = raw.get("LTP")

        if seg is None or not sec_id or ltp_raw is None:
            return

        try:
            ltp = float(ltp_raw)
        except (ValueError, TypeError):
            return

        # Resolve symbol name
        key = (int(seg), sec_id)
        symbol = self._instrument_to_symbol.get(
            key, self.scrip_master.get_symbol_name(int(seg), sec_id)
        )

        # Parse timestamp from LTT (HH:MM:SS)
        now = datetime.now()
        ltt_str = raw.get("LTT")
        if ltt_str and isinstance(ltt_str, str) and ":" in ltt_str:
            try:
                parts = [int(p) for p in ltt_str.split(":")]
                ts = now.replace(hour=parts[0], minute=parts[1], second=parts[2], microsecond=0)
            except Exception:
                ts = now
        else:
            ts = now

        volume = int(raw.get("volume", 0) or 0)
        open_val = float(raw["open"]) if "open" in raw and raw["open"] is not None else None
        high_val = float(raw["high"]) if "high" in raw and raw["high"] is not None else None
        low_val = float(raw["low"]) if "low" in raw and raw["low"] is not None else None
        close_val = float(raw["close"]) if "close" in raw and raw["close"] is not None else None

        tick = Tick(
            exchange_segment=int(seg),
            security_id=sec_id,
            symbol=symbol,
            ltp=ltp,
            timestamp=ts,
            volume=volume,
            open=open_val,
            high=high_val,
            low=low_val,
            close=close_val,
            avg_price=float(raw["avg_price"]) if "avg_price" in raw else None,
            oi=int(raw["OI"]) if "OI" in raw else None,
            raw=raw,
        )

        # Update latest caches
        exch_prefix = "NSE" if int(seg) == 1 else ("BSE" if int(seg) == 4 else ("IDX" if int(seg) == 0 else ""))
        with self._lock:
            self._ltp_cache[symbol] = ltp
            self._ltp_cache[sec_id] = ltp
            if exch_prefix:
                self._ltp_cache[f"{exch_prefix}:{symbol}"] = ltp
            self._tick_cache[symbol] = tick
            self._tick_cache[sec_id] = tick
            if exch_prefix:
                self._tick_cache[f"{exch_prefix}:{symbol}"] = tick

        # Dispatch tick to user callbacks
        for cb in self._tick_callbacks:
            try:
                cb(tick)
            except Exception as e:
                logger.error("Error in on_tick callback: %s", e)

        # Dispatch LTP callbacks
        for cb in self._global_ltp_callbacks:
            try:
                cb(symbol, ltp, ts)
            except Exception as e:
                logger.error("Error in global LTP callback: %s", e)

        specific_cbs = self._symbol_ltp_callbacks.get(symbol, [])
        for cb in specific_cbs:
            try:
                cb(symbol, ltp, ts)
            except Exception as e:
                logger.error("Error in symbol LTP callback (%s): %s", symbol, e)

        # Feed tick to Bar Aggregator
        self.bar_aggregator.process_tick(tick)

    def _dispatch_bar_close(self, bar: Bar) -> None:
        for cb in self._bar_close_callbacks:
            try:
                cb(bar)
            except Exception as e:
                logger.error("Error in bar close callback: %s", e)

    def _dispatch_bar_update(self, bar: Bar) -> None:
        for cb in self._bar_update_callbacks:
            try:
                cb(bar)
            except Exception as e:
                logger.error("Error in bar update callback: %s", e)

    # -------------------------------------------------------------------------
    # Cached Queries
    # -------------------------------------------------------------------------

    def get_ltp(self, symbol_or_id: str) -> float | None:
        """Get the most recent cached LTP for a symbol or security_id."""
        with self._lock:
            key = symbol_or_id.strip().upper()
            if key in self._ltp_cache:
                return self._ltp_cache[key]
            if str(symbol_or_id) in self._ltp_cache:
                return self._ltp_cache[str(symbol_or_id)]
            try:
                seg, sec_id, sym = self.scrip_master.resolve(key)
                seg_name = ExchangeSegment(seg).name
                return (
                    self._ltp_cache.get(key)
                    or self._ltp_cache.get(f"{seg_name}:{sym}")
                    or self._ltp_cache.get(sym)
                    or self._ltp_cache.get(sec_id)
                )
            except Exception:
                return None

    def get_latest_tick(self, symbol_or_id: str) -> Tick | None:
        """Get the most recent cached Tick for a symbol or security_id."""
        with self._lock:
            return self._tick_cache.get(symbol_or_id.strip().upper()) or self._tick_cache.get(str(symbol_or_id))

    def get_latest_bar(self, symbol: str, timeframe: str = TimeFrame.MINUTE_1) -> Bar | None:
        """Get the currently open in-progress Bar for a symbol and timeframe."""
        clean_sym = symbol.strip().upper()
        return self.bar_aggregator.get_latest_bar(clean_sym, timeframe)

    def get_bars(self, symbol: str, timeframe: str = TimeFrame.MINUTE_1, limit: int = 100) -> list[Bar]:
        """Get recent closed historical Bars for a symbol and timeframe."""
        clean_sym = symbol.strip().upper()
        return self.bar_aggregator.get_bars(clean_sym, timeframe, limit=limit)

    # -------------------------------------------------------------------------
    # Historical Bars via REST API
    # -------------------------------------------------------------------------

    def get_historical_minute_bars(
        self,
        symbol: str,
        from_date: str,
        to_date: str,
    ) -> list[dict]:
        """Fetch historical intraday 1-minute bars using DhanHQ REST API.

        Args:
            symbol: Symbol string (e.g. 'RELIANCE', 'TCS')
            from_date: Date string 'YYYY-MM-DD'
            to_date: Date string 'YYYY-MM-DD'
        """
        seg_val, sec_id, _ = self.scrip_master.resolve(symbol)
        ctx = self.auth_manager.get_dhan_context()
        hist = dhanhq.HistoricalData(ctx)

        # Map segment int to exchange segment string expected by Dhan REST API
        seg_name_map = {
            ExchangeSegment.IDX.value: "IDX_I",
            ExchangeSegment.NSE.value: "NSE_EQ",
            ExchangeSegment.NSE_FNO.value: "NSE_FNO",
            ExchangeSegment.BSE.value: "BSE_EQ",
            ExchangeSegment.MCX.value: "MCX_COMM",
        }
        exch_str = seg_name_map.get(seg_val, "NSE_EQ")

        res = hist.intraday_minute_data(
            security_id=sec_id,
            exchange_segment=exch_str,
            instrument_type="INDEX" if seg_val == ExchangeSegment.IDX.value else "EQUITY",
            from_date=from_date,
            to_date=to_date,
        )
        return res

    def get_historical_daily_bars(
        self,
        symbol: str,
        from_date: str,
        to_date: str,
    ) -> dict:
        """Fetch historical daily bars using DhanHQ REST API.

        Args:
            symbol: Symbol string (e.g. 'RELIANCE', 'TCS')
            from_date: Date string 'YYYY-MM-DD'
            to_date: Date string 'YYYY-MM-DD'
        """
        seg_val, sec_id, _ = self.scrip_master.resolve(symbol)
        ctx = self.auth_manager.get_dhan_context()
        hist = dhanhq.HistoricalData(ctx)

        seg_name_map = {
            ExchangeSegment.IDX.value: "IDX_I",
            ExchangeSegment.NSE.value: "NSE_EQ",
            ExchangeSegment.NSE_FNO.value: "NSE_FNO",
            ExchangeSegment.BSE.value: "BSE_EQ",
            ExchangeSegment.MCX.value: "MCX_COMM",
        }
        exch_str = seg_name_map.get(seg_val, "NSE_EQ")

        res = hist.historical_daily_data(
            security_id=sec_id,
            exchange_segment=exch_str,
            instrument_type="INDEX" if seg_val == ExchangeSegment.IDX.value else "EQUITY",
            from_date=from_date,
            to_date=to_date,
        )
        return res
