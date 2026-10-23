from __future__ import annotations

import csv
import io
import os
from pathlib import Path
from typing import NamedTuple
import urllib.request

from broker.models import ExchangeSegment


class InstrumentInfo(NamedTuple):
    symbol: str
    security_id: str
    exchange_segment: ExchangeSegment
    display_name: str
    instrument_type: str = "EQUITY"


class ScripMaster:
    """Helper to resolve trading symbols to Dhan (exchange_segment, security_id) pairs."""

    SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
    LOCAL_CACHE_PATH = Path("data/dhan/scrip_master.csv")
    JSON_CACHE_PATH = Path("data/dhan/nse_1000_stocks.json")
    FULL_UNIVERSE_JSON = Path("data/dhan/full_indian_universe.json")

    # Built-in popular symbols for immediate zero-latency resolution
    DEFAULT_SYMBOLS: dict[str, InstrumentInfo] = {
        # Indices
        "NIFTY": InstrumentInfo("NIFTY", "13", ExchangeSegment.IDX, "Nifty 50", "INDEX"),
        "NIFTY 50": InstrumentInfo("NIFTY 50", "13", ExchangeSegment.IDX, "Nifty 50", "INDEX"),
        "BANKNIFTY": InstrumentInfo("BANKNIFTY", "25", ExchangeSegment.IDX, "Nifty Bank", "INDEX"),
        "NIFTY BANK": InstrumentInfo("NIFTY BANK", "25", ExchangeSegment.IDX, "Nifty Bank", "INDEX"),
        "FINNIFTY": InstrumentInfo("FINNIFTY", "27", ExchangeSegment.IDX, "Nifty Financial Services", "INDEX"),
        "MIDCPNIFTY": InstrumentInfo("MIDCPNIFTY", "28", ExchangeSegment.IDX, "Nifty Midcap Select", "INDEX"),
        "SENSEX": InstrumentInfo("SENSEX", "51", ExchangeSegment.IDX, "BSE Sensex", "INDEX"),
        # Top Equities (NSE)
        "RELIANCE": InstrumentInfo("RELIANCE", "2885", ExchangeSegment.NSE, "Reliance Industries", "EQUITY"),
        "TCS": InstrumentInfo("TCS", "11536", ExchangeSegment.NSE, "Tata Consultancy Services", "EQUITY"),
        "HDFCBANK": InstrumentInfo("HDFCBANK", "1333", ExchangeSegment.NSE, "HDFC Bank Ltd", "EQUITY"),
        "INFY": InstrumentInfo("INFY", "1594", ExchangeSegment.NSE, "Infosys Ltd", "EQUITY"),
        "ICICIBANK": InstrumentInfo("ICICIBANK", "4963", ExchangeSegment.NSE, "ICICI Bank Ltd", "EQUITY"),
        "SBIN": InstrumentInfo("SBIN", "3045", ExchangeSegment.NSE, "State Bank of India", "EQUITY"),
        "BHARTIARTL": InstrumentInfo("BHARTIARTL", "10604", ExchangeSegment.NSE, "Bharti Airtel Ltd", "EQUITY"),
        "ITC": InstrumentInfo("ITC", "1660", ExchangeSegment.NSE, "ITC Ltd", "EQUITY"),
        "KOTAKBANK": InstrumentInfo("KOTAKBANK", "1922", ExchangeSegment.NSE, "Kotak Mahindra Bank", "EQUITY"),
        "LT": InstrumentInfo("LT", "11483", ExchangeSegment.NSE, "Larsen & Toubro Ltd", "EQUITY"),
        "AXISBANK": InstrumentInfo("AXISBANK", "5900", ExchangeSegment.NSE, "Axis Bank Ltd", "EQUITY"),
        "TATAMOTORS": InstrumentInfo("TATAMOTORS", "3456", ExchangeSegment.NSE, "Tata Motors Ltd", "EQUITY"),
        "MARUTI": InstrumentInfo("MARUTI", "10999", ExchangeSegment.NSE, "Maruti Suzuki Ltd", "EQUITY"),
        "HINDUNILVR": InstrumentInfo("HINDUNILVR", "1394", ExchangeSegment.NSE, "Hindustan Unilever Ltd", "EQUITY"),
    }

    def __init__(self, auto_load_cache: bool = True):
        self._cache: dict[str, InstrumentInfo] = dict(self.DEFAULT_SYMBOLS)
        self._reverse_map: dict[tuple[int, str], str] = {
            (info.exchange_segment.value, info.security_id): info.symbol
            for info in self.DEFAULT_SYMBOLS.values()
        }
        if auto_load_cache:
            if self.FULL_UNIVERSE_JSON.exists():
                self._load_from_json(self.FULL_UNIVERSE_JSON)
            elif self.JSON_CACHE_PATH.exists():
                self._load_from_json(self.JSON_CACHE_PATH)
            if self.LOCAL_CACHE_PATH.exists():
                self._load_from_csv(self.LOCAL_CACHE_PATH)

    def resolve(self, symbol_or_tuple: str | tuple) -> tuple[int, str, str]:
        """Resolve various representations into (exchange_segment_int, security_id_str, symbol_str).

        Supported inputs:
            - "RELIANCE" -> (1, "2885", "RELIANCE")
            - "NSE:RELIANCE" -> (1, "2885", "RELIANCE")
            - "BSE:500325" or "BSE:ABB" -> (4, "500002", "ABB")
            - "IDX:13" or "IDX:NIFTY" -> (0, "13", "NIFTY")
            - (ExchangeSegment.NSE, "2885") -> (1, "2885", "RELIANCE")
            - (1, "2885") -> (1, "2885", "RELIANCE")
        """
        if isinstance(symbol_or_tuple, tuple):
            if len(symbol_or_tuple) >= 2:
                seg, sec_id = symbol_or_tuple[0], str(symbol_or_tuple[1])
                seg_int = int(seg)
                sym = self._reverse_map.get((seg_int, sec_id), f"{seg_int}:{sec_id}")
                return seg_int, sec_id, sym

        raw_str = str(symbol_or_tuple).strip()
        clean = raw_str.upper()

        # 1. Exact match in cache (e.g. "NSE:RELIANCE", "BSE:ABB", "RELIANCE", "NIFTY")
        if clean in self._cache:
            info = self._cache[clean]
            return info.exchange_segment.value, info.security_id, info.symbol

        # 2. Check if format is "SEGMENT:SYMBOL_OR_ID" (e.g. "NSE:2885" or "BSE:500325")
        if ":" in raw_str:
            seg_str, sym_part = raw_str.split(":", 1)
            try:
                seg = ExchangeSegment.from_str(seg_str)
                # If sym_part is numeric security ID
                if sym_part.isdigit():
                    sec_id = sym_part
                    sym = self._reverse_map.get((seg.value, sec_id), raw_str)
                    return seg.value, sec_id, sym

                # Check if segment-prefixed ticker is in cache
                prefixed_key = f"{seg.name}:{sym_part.upper()}"
                if prefixed_key in self._cache:
                    info = self._cache[prefixed_key]
                    return info.exchange_segment.value, info.security_id, info.symbol

                # Check if sym_part alone is in cache
                key = sym_part.upper()
                if key in self._cache:
                    info = self._cache[key]
                    return seg.value, info.security_id, info.symbol
                return seg.value, sym_part, raw_str
            except ValueError:
                pass

        raise ValueError(
            f"Cannot resolve symbol '{symbol_or_tuple}'. "
            f"Pass as (exchange_segment, security_id) or check ScripMaster cache."
        )

    def register_symbol(
        self,
        symbol: str,
        security_id: str,
        exchange_segment: ExchangeSegment | int,
        display_name: str = "",
    ) -> None:
        """Register a custom symbol mapping."""
        seg = ExchangeSegment(int(exchange_segment))
        info = InstrumentInfo(
            symbol=symbol.upper(),
            security_id=str(security_id),
            exchange_segment=seg,
            display_name=display_name or symbol,
        )
        self._cache[info.symbol] = info
        self._reverse_map[(seg.value, info.security_id)] = info.symbol

    def get_symbol_name(self, exchange_segment: int, security_id: str) -> str:
        """Find symbol name for a given segment and security id."""
        return self._reverse_map.get((exchange_segment, str(security_id)), f"{exchange_segment}:{security_id}")

    def _load_from_csv(self, path: Path) -> None:
        try:
            with open(path, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    exch = row.get("SEM_EXM_EXCH_ID") or row.get("EXCH_ID")
                    sec_id = row.get("SEM_SMST_SECURITY_ID") or row.get("SECURITY_ID")
                    sym = row.get("SEM_TRADING_SYMBOL") or row.get("TRADING_SYMBOL")
                    if exch and sec_id and sym:
                        try:
                            seg = ExchangeSegment.from_str(exch)
                            self.register_symbol(sym, sec_id, seg)
                        except Exception:
                            continue
        except Exception as e:
            print(f"[ScripMaster] Notice: failed to load local cache from {path}: {e}")

    def _load_from_json(self, path: Path) -> None:
        try:
            import json
            with open(path, mode="r", encoding="utf-8") as f:
                data = json.load(f)
                items = data.get("stocks", data) if isinstance(data, dict) else data
                for item in items:
                    sym = item.get("symbol")
                    sec_id = item.get("security_id")
                    seg_val = item.get("exchange_segment", 1)
                    exch = item.get("exchange", "")
                    name = item.get("name", sym)
                    if sym and sec_id:
                        # Register prefixed form (e.g. "NSE:RELIANCE", "BSE:ABB")
                        if exch:
                            self.register_symbol(f"{exch}:{sym}", sec_id, seg_val, display_name=name)
                        # Register bare symbol if not already present or if NSE (primary equity exchange)
                        if sym not in self._cache or exch == "NSE":
                            self.register_symbol(sym, sec_id, seg_val, display_name=name)
                        # Always register reverse map for this instrument
                        self._reverse_map[(int(seg_val), str(sec_id))] = sym
        except Exception as e:
            print(f"[ScripMaster] Notice: failed to load JSON cache from {path}: {e}")

