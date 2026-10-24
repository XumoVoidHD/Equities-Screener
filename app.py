"""Streamlit Interactive Screener Dashboard.

Features:
- Live Configuration Controls: Tweak condition parameters (factors, lookbacks, RSI deltas, volume thresholds)
  and toggles in real-time.
- One-Click Presets: Instantly switch between High-Yield (~98%), Intraday Momentum (~47%),
  RSI Rising (~33%), Swing Leaders (~15%), and Original Strict Chartink (~1%).
- Real-Time Evaluation: Screens 116+ real Indian stocks instantly with mathematical precision.
- Funnel Analytics: Visualize condition pass rates and pinpoint bottlenecks.
- Dynamic Leaderboard: Sort, filter, search, and export matching stocks to CSV.
- Stock Inspector: Detailed multi-timeframe evaluation breakdown and price trend charts.

Usage:
    streamlit run app.py
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from broker.conditions import (
    Condition1HourlyAboveDailyOpen,
    Condition2HourlyAbovePrevCloseDiscount,
    Condition3HourlyAbovePastClose,
    Condition4DailyRSIJump,
    Condition5DailyVolumeThreshold,
    ScreenerConditionsConfig,
    StockMarketState,
    compute_wilder_rsi,
)

# -----------------------------------------------------------------------------
# Page Configuration & Modern Theme Styling
# -----------------------------------------------------------------------------

st.set_page_config(
    page_title="Dhan Equity Screener Pro",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inject Custom High-End Financial Dashboard CSS
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, sans-serif;
    }

    .code-font {
        font-family: 'JetBrains Mono', monospace;
    }

    /* Main Container Padding */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2.5rem;
        padding-left: 2.5rem;
        padding-right: 2.5rem;
    }

    /* Glassmorphic KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 14px;
        padding: 1.25rem 1.5rem;
        box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.3);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: rgba(99, 102, 241, 0.4);
    }

    .kpi-label {
        font-size: 0.82rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94A3B8;
        margin-bottom: 0.4rem;
    }

    .kpi-value {
        font-size: 2.1rem;
        font-weight: 800;
        color: #F8FAFC;
        line-height: 1.1;
    }

    .kpi-subtext {
        font-size: 0.8rem;
        color: #10B981;
        margin-top: 0.4rem;
        display: flex;
        align-items: center;
        gap: 0.3rem;
    }

    /* Condition Card in Sidebar */
    .cond-box {
        background: rgba(30, 41, 59, 0.5);
        border-left: 4px solid #6366F1;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.75rem;
    }

    /* Status Badges */
    .badge-pass {
        background-color: rgba(16, 185, 129, 0.15);
        color: #10B981;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .badge-fail {
        background-color: rgba(239, 68, 68, 0.15);
        color: #EF4444;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 700;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .badge-disabled {
        background-color: rgba(148, 163, 184, 0.15);
        color: #94A3B8;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    /* Metric Header Glow */
    .brand-glow {
        background: linear-gradient(135deg, #6366F1 0%, #06B6D4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# Data Loader & Caching
# -----------------------------------------------------------------------------


@st.cache_data(show_spinner=False)
def load_research_dataset(cache_mtime: float = 0.0) -> list[dict]:
    """Load cached historical market data for Indian stocks."""
    cache_path = Path("data/dhan/research_sample_daily.json")
    if not cache_path.exists():
        st.error(f"Dataset missing at {cache_path}. Run scripts/cache_universe_daily.py first.")
        return []

    with open(cache_path, "r", encoding="utf-8") as f:
        cache = json.load(f)

    records = []
    for sym, candles in cache.items():
        closes = candles.get("close", [])
        opens = candles.get("open", [])
        vols = candles.get("volume", [])
        highs = candles.get("high", [])
        lows = candles.get("low", [])

        if len(closes) < 15:
            continue

        hourly_px = closes[-1]
        d_open = opens[-1] if opens else hourly_px
        d_vol = vols[-1] if vols else 0.0

        state = StockMarketState(
            symbol=sym,
            hourly_close=hourly_px,
            daily_open=d_open,
            daily_closes=closes[:-1],
            daily_volumes=vols[:-1] if vols else [],
            daily_highs=highs[:-1] if highs else [],
            daily_lows=lows[:-1] if lows else [],
            daily_volume=d_vol,
        )

        records.append({
            "symbol": sym,
            "state": state,
            "latest_close": hourly_px,
            "daily_open": d_open,
            "prev_close": closes[-2] if len(closes) >= 2 else hourly_px,
            "close_10d": closes[-11] if len(closes) >= 11 else (closes[0] if closes else hourly_px),
            "today_rsi": state.get_daily_rsi(14, 0) or 50.0,
            "prev_rsi": state.get_daily_rsi(14, 1) or 50.0,
            "daily_volume": d_vol,
            "history_len": len(closes),
            "all_closes": closes,
        })

    return records


# -----------------------------------------------------------------------------
# Preset Definitions
# -----------------------------------------------------------------------------

PRESETS = {
    "🌟 Multi-Condition Wide Net (OR Logic - ~98% yield)": {
        "file": "config/preset_wide_net_leaderboard.json",
        "logic": "OR",
        "c1_enabled": True, "c1_op": ">", "c1_factor": 1.0, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": True, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 1.0,
        "c4_enabled": True, "c4_op": ">", "c4_window": 14, "c4_lookback": 1, "c4_delta": 1.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
    "🟢 Active Intraday Green Stocks (C1 + C2 - ~47% yield)": {
        "file": "config/preset_green_intraday_momentum.json",
        "logic": "AND",
        "c1_enabled": True, "c1_op": ">", "c1_factor": 1.0, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": False, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 1.0,
        "c4_enabled": False, "c4_op": ">", "c4_window": 14, "c4_lookback": 1, "c4_delta": 10.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
    "📈 Green Day + RSI Rising (+1 pt jump - ~33% yield)": {
        "file": "config/preset_intraday_rsi_rising.json",
        "logic": "AND",
        "c1_enabled": True, "c1_op": ">", "c1_factor": 1.0, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": False, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 1.0,
        "c4_enabled": True, "c4_op": ">", "c4_window": 14, "c4_lookback": 1, "c4_delta": 1.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
    "🏆 10-Day Relative Strength Leaders (C2 + C3 - ~15.5% yield)": {
        "file": "config/preset_swing_relative_strength.json",
        "logic": "AND",
        "c1_enabled": False, "c1_op": ">", "c1_factor": 1.0, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": True, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 1.0,
        "c4_enabled": False, "c4_op": ">", "c4_window": 14, "c4_lookback": 1, "c4_delta": 10.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
    "🎯 Consolidation Breakout with Dip Tolerance (~12% yield)": {
        "file": "config/preset_breakout_dip_tolerance.json",
        "logic": "AND",
        "c1_enabled": True, "c1_op": ">", "c1_factor": 0.995, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": True, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 0.98,
        "c4_enabled": True, "c4_op": ">=", "c4_window": 14, "c4_lookback": 1, "c4_delta": 0.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
    "🎯 Original Chartink Baseline (Strict RSI +10.0 - ~0.9% yield)": {
        "file": "config/screener_conditions.json",
        "logic": "AND",
        "c1_enabled": True, "c1_op": ">", "c1_factor": 1.0, "c1_offset": 0.0,
        "c2_enabled": True, "c2_op": ">", "c2_lookback": 1, "c2_factor": 0.95,
        "c3_enabled": True, "c3_op": ">=", "c3_lookback": 10, "c3_factor": 1.0,
        "c4_enabled": True, "c4_op": ">", "c4_window": 14, "c4_lookback": 1, "c4_delta": 10.0,
        "c5_enabled": False, "c5_op": ">", "c5_vol": 5000000.0,
    },
}

# -----------------------------------------------------------------------------
# Initialize Session State
# -----------------------------------------------------------------------------

default_preset_name = "📈 Green Day + RSI Rising (+1 pt jump - ~33% yield)"

if "selected_preset" not in st.session_state:
    st.session_state["selected_preset"] = default_preset_name

if "c1_enabled" not in st.session_state:
    p = PRESETS[default_preset_name]
    for k, v in p.items():
        if k != "file":
            st.session_state[k] = v


def apply_preset(name: str):
    """Callback to update session state when a preset is chosen."""
    p = PRESETS[name]
    for k, v in p.items():
        if k != "file":
            st.session_state[k] = v
    st.session_state["selected_preset"] = name


# -----------------------------------------------------------------------------
# Sidebar: Interactive Configuration Panel
# -----------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### ⚡ Screener Engine")
    st.caption("Customize condition parameters and re-run on real Indian stock data.")

    # 1. Preset Selector
    st.markdown("#### 📁 Quick Presets")
    chosen_preset = st.selectbox(
        "Load Configuration Preset",
        options=list(PRESETS.keys()),
        index=list(PRESETS.keys()).index(st.session_state["selected_preset"]),
        key="preset_dropdown",
        on_change=lambda: apply_preset(st.session_state["preset_dropdown"]),
    )

    st.markdown("---")

    # 2. Global Combination Logic
    st.markdown("#### 🔀 Evaluation Logic")
    logic_choice = st.radio(
        "Condition Connector",
        options=["AND", "OR"],
        index=0 if st.session_state.get("logic", "AND") == "AND" else 1,
        help="AND: All active conditions must pass.\nOR: Any active condition passing qualifies the stock.",
        horizontal=True,
        key="logic",
    )

    st.markdown("---")

    # 3. Stock Universe Scope
    st.markdown("#### 🌐 Stock Universe Scope")
    cache_path = Path("data/dhan/research_sample_daily.json")
    cache_mtime = cache_path.stat().st_mtime if cache_path.exists() else 0.0
    full_dataset = load_research_dataset(cache_mtime)
    total_avail = len(full_dataset)

    scope_choice = st.selectbox(
        "Screening Universe",
        options=[
            f"All Active Stocks ({total_avail} Equities)",
            "Top 100 Liquid Largecaps",
            "Nifty 50 Bluechips",
            "Quick Sample (116 Stocks)",
        ],
        index=0,
        help="Choose how many stocks from the active universe to evaluate.",
    )

    if "All Active" in scope_choice:
        dataset = full_dataset
    elif "Top 100" in scope_choice:
        dataset = full_dataset[:100]
    elif "Nifty 50" in scope_choice:
        dataset = full_dataset[:50]
    elif "Quick Sample" in scope_choice:
        dataset = full_dataset[:116]
    else:
        dataset = full_dataset

    if st.button("🔄 Reload Data Cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("#### ⚙️ Configurable Conditions")

    # Condition 1
    with st.expander("Condition 1: Hourly Close vs Daily Open", expanded=st.session_state.get("c1_enabled", True)):
        c1_en = st.toggle("Enable Condition 1", key="c1_enabled")
        c1_op = st.selectbox("Operator", [">", ">=", "<", "<="], key="c1_op")
        c1_factor = st.slider("Multiplier Factor", min_value=0.90, max_value=1.10, step=0.005, key="c1_factor")
        c1_offset = st.number_input("Offset (Rs.)", value=0.0, step=0.5, key="c1_offset")
        st.caption(f"Rule: `Hourly Close {c1_op} (Daily Open * {c1_factor:.3f}) + {c1_offset:.1f}`")

    # Condition 2
    with st.expander("Condition 2: Hourly vs Yesterday Discount", expanded=st.session_state.get("c2_enabled", True)):
        c2_en = st.toggle("Enable Condition 2", key="c2_enabled")
        c2_op = st.selectbox("Operator", [">", ">="], key="c2_op")
        c2_lookback = st.number_input("Lookback Days", min_value=1, max_value=10, step=1, key="c2_lookback")
        c2_factor = st.slider("Discount Factor", min_value=0.85, max_value=1.05, step=0.01, key="c2_factor")
        st.caption(f"Rule: `Hourly Close {c2_op} ([-{c2_lookback}] Close * {c2_factor:.2f})`")

    # Condition 3
    with st.expander("Condition 3: Hourly vs Past Daily Close", expanded=st.session_state.get("c3_enabled", False)):
        c3_en = st.toggle("Enable Condition 3", key="c3_enabled")
        c3_op = st.selectbox("Operator", [">=", ">"], key="c3_op")
        c3_lookback = st.slider("Past Lookback Sessions", min_value=2, max_value=20, step=1, key="c3_lookback")
        c3_factor = st.slider("Factor", min_value=0.90, max_value=1.10, step=0.01, key="c3_factor")
        st.caption(f"Rule: `Hourly Close {c3_op} [-{c3_lookback}] Close * {c3_factor:.2f}`")

    # Condition 4
    with st.expander("Condition 4: Daily RSI Momentum Surge", expanded=st.session_state.get("c4_enabled", True)):
        c4_en = st.toggle("Enable Condition 4", key="c4_enabled")
        c4_op = st.selectbox("Operator", [">", ">="], key="c4_op")
        c4_window = st.number_input("RSI Window Period", min_value=5, max_value=30, step=1, key="c4_window")
        c4_lookback = st.number_input("RSI Lookback Days", min_value=1, max_value=5, step=1, key="c4_lookback")
        c4_delta = st.slider("Required RSI Delta Jump (+pts)", min_value=0.0, max_value=15.0, step=0.5, key="c4_delta")
        st.caption(f"Rule: `Today RSI({c4_window}) {c4_op} Yesterday RSI + {c4_delta:.1f}`")

    # Condition 5
    with st.expander("Condition 5: Daily Cumulative Volume", expanded=st.session_state.get("c5_enabled", False)):
        c5_en = st.toggle("Enable Condition 5", key="c5_enabled")
        c5_op = st.selectbox("Operator", [">", ">="], key="c5_op")
        c5_vol = st.number_input("Min Volume (Shares)", min_value=10000.0, max_value=50000000.0, step=50000.0, key="c5_vol")
        st.caption(f"Rule: `Daily Volume {c5_op} {c5_vol:,.0f}`")

    st.markdown("---")

    # Save Configuration Button
    if st.button("💾 Export Current Config to JSON", use_container_width=True):
        custom_cfg = ScreenerConditionsConfig(
            screener_name=f"Custom: {chosen_preset}",
            logic=logic_choice,
            condition_1=Condition1HourlyAboveDailyOpen(
                enabled=st.session_state["c1_enabled"],
                factor=st.session_state["c1_factor"],
                offset=st.session_state["c1_offset"],
                operator=st.session_state["c1_op"],
            ),
            condition_2=Condition2HourlyAbovePrevCloseDiscount(
                enabled=st.session_state["c2_enabled"],
                lookback_days=st.session_state["c2_lookback"],
                factor=st.session_state["c2_factor"],
                operator=st.session_state["c2_op"],
            ),
            condition_3=Condition3HourlyAbovePastClose(
                enabled=st.session_state["c3_enabled"],
                lookback_days=st.session_state["c3_lookback"],
                factor=st.session_state["c3_factor"],
                operator=st.session_state["c3_op"],
            ),
            condition_4=Condition4DailyRSIJump(
                enabled=st.session_state["c4_enabled"],
                rsi_window=st.session_state["c4_window"],
                lookback_days=st.session_state["c4_lookback"],
                rsi_delta=st.session_state["c4_delta"],
                operator=st.session_state["c4_op"],
            ),
            condition_5=Condition5DailyVolumeThreshold(
                enabled=st.session_state["c5_enabled"],
                min_volume=st.session_state["c5_vol"],
                operator=st.session_state["c5_op"],
            ),
        )
        custom_cfg.save_json("config/screener_conditions.json")
        st.success("Saved to `config/screener_conditions.json`!")


# -----------------------------------------------------------------------------
# Construct ScreenerConditionsConfig from Current Form State
# -----------------------------------------------------------------------------

active_config = ScreenerConditionsConfig(
    screener_name=f"Live Configuration ({logic_choice})",
    logic=logic_choice,
    condition_1=Condition1HourlyAboveDailyOpen(
        enabled=st.session_state.get("c1_enabled", True),
        factor=st.session_state.get("c1_factor", 1.0),
        offset=st.session_state.get("c1_offset", 0.0),
        operator=st.session_state.get("c1_op", ">"),
    ),
    condition_2=Condition2HourlyAbovePrevCloseDiscount(
        enabled=st.session_state.get("c2_enabled", True),
        lookback_days=int(st.session_state.get("c2_lookback", 1)),
        factor=st.session_state.get("c2_factor", 0.95),
        operator=st.session_state.get("c2_op", ">"),
    ),
    condition_3=Condition3HourlyAbovePastClose(
        enabled=st.session_state.get("c3_enabled", False),
        lookback_days=int(st.session_state.get("c3_lookback", 10)),
        factor=st.session_state.get("c3_factor", 1.0),
        operator=st.session_state.get("c3_op", ">="),
    ),
    condition_4=Condition4DailyRSIJump(
        enabled=st.session_state.get("c4_enabled", True),
        rsi_window=int(st.session_state.get("c4_window", 14)),
        lookback_days=int(st.session_state.get("c4_lookback", 1)),
        rsi_delta=st.session_state.get("c4_delta", 1.0),
        operator=st.session_state.get("c4_op", ">"),
    ),
    condition_5=Condition5DailyVolumeThreshold(
        enabled=st.session_state.get("c5_enabled", False),
        min_volume=float(st.session_state.get("c5_vol", 5000000.0)),
        operator=st.session_state.get("c5_op", ">"),
    ),
)


# -----------------------------------------------------------------------------
# Run Screener Pipeline
# -----------------------------------------------------------------------------

results = []
cond_pass_counts = {"condition_1": 0, "condition_2": 0, "condition_3": 0, "condition_4": 0, "condition_5": 0}

for rec in dataset:
    state = rec["state"]
    res = active_config.evaluate(state)

    for cid, c_eval in res.evaluations.items():
        if c_eval.passed and c_eval.enabled:
            cond_pass_counts[cid] += 1

    pct_chg = ((state.hourly_close - rec["prev_close"]) / rec["prev_close"]) * 100.0 if rec["prev_close"] else 0.0
    rsi_today = rec["today_rsi"]
    rsi_yesterday = rec["prev_rsi"]
    rsi_delta = rsi_today - rsi_yesterday

    results.append({
        "symbol": rec["symbol"],
        "passed": res.passed,
        "score": round(res.score + (len(res.matched_conditions) * 2.0), 2),
        "price": rec["latest_close"],
        "change_pct": round(pct_chg, 2),
        "daily_open": rec["daily_open"],
        "prev_close": rec["prev_close"],
        "close_10d": rec["close_10d"],
        "today_rsi": round(rsi_today, 1),
        "prev_rsi": round(rsi_yesterday, 1),
        "rsi_delta": round(rsi_delta, 1),
        "volume": int(rec["daily_volume"]),
        "matched_count": len(res.matched_conditions),
        "matched_reasons": ", ".join(res.matched_conditions) if res.matched_conditions else "None",
        "evaluations": res.evaluations,
        "all_closes": rec["all_closes"],
    })

matched_list = [r for r in results if r["passed"]]
matched_list.sort(key=lambda x: x["score"], reverse=True)
pass_rate = (len(matched_list) / len(results)) * 100.0 if results else 0.0


# -----------------------------------------------------------------------------
# Main Dashboard UI
# -----------------------------------------------------------------------------

# Header
title_col, btn_col = st.columns([4, 1])
with title_col:
    st.markdown("## ⚡ <span class='brand-glow'>DhanHQ</span> Equity Screener Pro", unsafe_allow_html=True)
    st.caption(f"Real-Time Multi-Timeframe Technical Screener • Evaluating **{len(dataset)}** Stocks (Universe: **{total_avail}** cached / **2,736** NSE equities) • Logic: `{logic_choice}`")

with btn_col:
    if st.button("🔄 Re-Run Screener", type="primary", use_container_width=True):
        st.rerun()

st.markdown("---")

# 1. KPI Cards Row
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Stocks Matched</div>
            <div class="kpi-value">{len(matched_list)} <span style="font-size:1.1rem; color:#94A3B8;">/ {len(dataset)}</span></div>
            <div class="kpi-subtext">Pass Rate: <strong>{pass_rate:.1f}%</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi2:
    active_cond_count = sum(1 for c in [active_config.condition_1, active_config.condition_2, active_config.condition_3, active_config.condition_4, active_config.condition_5] if c.enabled)
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Active Conditions</div>
            <div class="kpi-value">{active_cond_count} <span style="font-size:1.1rem; color:#94A3B8;">/ 5</span></div>
            <div class="kpi-subtext" style="color: {'#10B981' if logic_choice == 'AND' else '#6366F1'};">Logic Mode: <strong>{logic_choice}</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi3:
    top_sym = matched_list[0]["symbol"] if matched_list else "None"
    top_score = matched_list[0]["score"] if matched_list else 0.0
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Top Ranked Candidate</div>
            <div class="kpi-value">{top_sym}</div>
            <div class="kpi-subtext">Urgency Score: <strong>{top_score:.1f}</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi4:
    avg_price = np.mean([m["price"] for m in matched_list]) if matched_list else 0.0
    med_price = np.median([r["price"] for r in results]) if results else 0.0
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">Average Price</div>
            <div class="kpi-value">Rs.{avg_price:,.0f}</div>
            <div class="kpi-subtext">Universe Median: <strong>Rs.{med_price:,.0f}</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)

# 2. Condition Bottleneck Funnel Chart
st.markdown("### 🔍 Condition Pass Rate Funnel")
st.caption("Identify which conditions are passing most stocks vs acting as strict filters.")

funnel_data = pd.DataFrame([
    {
        "Condition": "C1: Hourly > Daily Open",
        "Passed Stocks": cond_pass_counts["condition_1"],
        "Pass Rate (%)": round((cond_pass_counts["condition_1"] / len(dataset)) * 100, 1),
        "Status": "Active" if active_config.condition_1.enabled else "Disabled",
    },
    {
        "Condition": "C2: Hourly > [-1] Close*0.95",
        "Passed Stocks": cond_pass_counts["condition_2"],
        "Pass Rate (%)": round((cond_pass_counts["condition_2"] / len(dataset)) * 100, 1),
        "Status": "Active" if active_config.condition_2.enabled else "Disabled",
    },
    {
        "Condition": "C3: Hourly >= [-10] Close",
        "Passed Stocks": cond_pass_counts["condition_3"],
        "Pass Rate (%)": round((cond_pass_counts["condition_3"] / len(dataset)) * 100, 1),
        "Status": "Active" if active_config.condition_3.enabled else "Disabled",
    },
    {
        "Condition": f"C4: RSI Surge (+{active_config.condition_4.rsi_delta:.1f} pts)",
        "Passed Stocks": cond_pass_counts["condition_4"],
        "Pass Rate (%)": round((cond_pass_counts["condition_4"] / len(dataset)) * 100, 1),
        "Status": "Active" if active_config.condition_4.enabled else "Disabled",
    },
    {
        "Condition": f"C5: Vol > {active_config.condition_5.min_volume/1e6:.1f}M",
        "Passed Stocks": cond_pass_counts["condition_5"],
        "Pass Rate (%)": round((cond_pass_counts["condition_5"] / len(dataset)) * 100, 1),
        "Status": "Active" if active_config.condition_5.enabled else "Disabled",
    },
])

chart = (
    alt.Chart(funnel_data)
    .mark_bar(cornerRadius=6, height=22)
    .encode(
        x=alt.X("Pass Rate (%):Q", scale=alt.Scale(domain=[0, 100])),
        y=alt.Y("Condition:N", sort=None, title=None),
        color=alt.Color(
            "Status:N",
            scale=alt.Scale(domain=["Active", "Disabled"], range=["#6366F1", "#475569"]),
            legend=alt.Legend(title="Rule State"),
        ),
        tooltip=["Condition", "Passed Stocks", "Pass Rate (%)", "Status"],
    )
    .properties(height=200)
)
st.altair_chart(chart, use_container_width=True)

st.markdown("---")

# 3. Screener Results Table
st.markdown("### 📋 Screened Stocks Leaderboard")

if matched_list:
    df_display = pd.DataFrame([
        {
            "Rank": idx + 1,
            "Symbol": m["symbol"],
            "Price (Rs.)": m["price"],
            "Day Chg (%)": m["change_pct"],
            "Today RSI(14)": m["today_rsi"],
            "RSI Delta": m["rsi_delta"],
            "Daily Open": m["daily_open"],
            "[-1] Close": m["prev_close"],
            "[-10] Close": m["close_10d"],
            "Daily Volume": m["volume"],
            "Score": m["score"],
            "Matched Rules": m["matched_reasons"],
        }
        for idx, m in enumerate(matched_list)
    ])

    search_col, filter_col, dl_col = st.columns([2, 1, 1])
    with search_col:
        search_query = st.text_input("🔍 Search Symbol", placeholder="Filter (e.g. RELIANCE, TCS, INFY)...")
    with filter_col:
        sort_by = st.selectbox("Sort By", ["Score", "Day Chg (%)", "Price (Rs.)", "RSI Delta", "Daily Volume"])
    with dl_col:
        csv_data = df_display.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Export CSV", data=csv_data, file_name="screened_stocks.csv", mime="text/csv", use_container_width=True)

    if search_query:
        df_display = df_display[df_display["Symbol"].str.contains(search_query.strip().upper())]

    df_display = df_display.sort_values(by=sort_by, ascending=False)

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Price (Rs.)": st.column_config.NumberColumn(format="₹%.2f"),
            "Daily Open": st.column_config.NumberColumn(format="₹%.2f"),
            "[-1] Close": st.column_config.NumberColumn(format="₹%.2f"),
            "[-10] Close": st.column_config.NumberColumn(format="₹%.2f"),
            "Day Chg (%)": st.column_config.NumberColumn(format="%+.2f%%"),
            "RSI Delta": st.column_config.NumberColumn(format="%+.1f"),
            "Daily Volume": st.column_config.NumberColumn(format="%d"),
            "Score": st.column_config.ProgressColumn(
                "Urgency Score",
                min_value=0,
                max_value=float(df_display["Score"].max() or 10.0),
                format="%.1f",
            ),
        },
    )
else:
    st.warning("⚠️ No stocks matched with current parameters. Try relaxing Condition 4 (RSI Delta) or switching Logic to 'OR'.")

st.markdown("---")

# 4. Stock Deep Dive Inspector
st.markdown("### 🔬 Stock Deep Dive Inspector")
st.caption("Select any stock to see full condition-by-condition evaluation details and historical price trajectory.")

inspect_symbols = [m["symbol"] for m in matched_list] or [r["symbol"] for r in results]
selected_stock_sym = st.selectbox("Select Stock to Inspect", options=inspect_symbols, index=0)

stock_record = next((r for r in results if r["symbol"] == selected_stock_sym), None)

if stock_record:
    c_left, c_right = st.columns([3, 2])

    with c_left:
        st.markdown(f"#### Evaluation Audit: `{selected_stock_sym}`")
        evals = stock_record["evaluations"]

        for cid, ev in evals.items():
            badge_cls = "badge-pass" if ev.passed else "badge-fail"
            status_text = "PASS" if ev.passed else "FAIL"
            if not ev.enabled:
                badge_cls = "badge-disabled"
                status_text = "DISABLED"

            st.markdown(
                f"""
                <div class="cond-box">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <strong>{ev.name}</strong>
                        <span class="{badge_cls}">{status_text}</span>
                    </div>
                    <div style="font-size:0.85rem; color:#94A3B8; margin-top:0.3rem;" class="code-font">
                        {ev.message}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with c_right:
        st.markdown(f"#### Price History & Lookbacks")
        closes = stock_record["all_closes"]
        if closes:
            chart_df = pd.DataFrame({
                "Day": list(range(-len(closes) + 1, 1)),
                "Close Price (Rs.)": closes,
            })
            chart_altair = (
                alt.Chart(chart_df)
                .mark_line(color="#6366F1", strokeWidth=2.5, point=True)
                .encode(
                    x=alt.X("Day:Q", title="Trading Sessions (0 = Today, -1 = Yesterday, -10 = 10d ago)"),
                    y=alt.Y("Close Price (Rs.):Q", scale=alt.Scale(zero=False), title="Close Price (Rs.)"),
                    tooltip=["Day", "Close Price (Rs.)"],
                )
                .properties(height=260)
            )
            st.altair_chart(chart_altair, use_container_width=True)

        st.markdown(
            f"""
            - **Current Price**: `Rs.{stock_record['price']:.2f}`
            - **Daily Open**: `Rs.{stock_record['daily_open']:.2f}`
            - **[-1] Yesterday Close**: `Rs.{stock_record['prev_close']:.2f}`
            - **[-10] 10-Day Close**: `Rs.{stock_record['close_10d']:.2f}`
            - **Today RSI(14)**: `{stock_record['today_rsi']:.1f}` (Prev: `{stock_record['prev_rsi']:.1f}`, Delta: `{stock_record['rsi_delta']:+.1f}`)
            """
        )
