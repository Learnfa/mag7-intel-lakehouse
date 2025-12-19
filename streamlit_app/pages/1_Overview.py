# pages/overview.py
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.data_loaders import (
    load_overview_signal_snapshot,
    load_overview_macro_snapshot,
    load_overview_today,
    load_overview_trending,
)
from components.banners import production_truth_banner
from components.metrics import kpi_row
from components.freshness import data_freshness_panel
from components.gauges import fear_greed_dial
from utils.constants import S0_SIGNAL_COLORS

st.set_page_config(
    page_title="Overview | Market Control Center",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Market Overview")
st.caption("High-level market orientation • Signals, trend, sentiment, and risk")

production_truth_banner()

# ---------------------------------------------------------------------
# Load snapshots
# ---------------------------------------------------------------------
with st.spinner("Loading overview snapshots…"):
    snap_signal = load_overview_signal_snapshot()
    snap_macro = load_overview_macro_snapshot()
    today_df = load_overview_today()

if snap_signal.empty or snap_macro.empty or today_df.empty:
    st.error("No overview data available (snapshot tables returned empty).")
    st.stop()

asof_date = pd.to_datetime(snap_signal.iloc[0]["asof_date"])

data_freshness_panel(
    asof_date=asof_date,
    sources=[
        "mag7_intel_mart.s0_core_value",
        "mag7_intel_mart.overview_today",
        "mag7_intel_mart.macro_risk_ts",
        "mag7_intel_mart.market_sentiment_ts",
        "mag7_intel_core.fact_price_features",
    ],
    location="sidebar",
)

# ---------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")

    window = st.selectbox(
        "Time window",
        ["3m", "6m", "1y", "2y", "max"],
        index=2,
    )

    price_mode = st.radio(
        "Chart mode",
        ["Price", "Indexed (100)", "Cumulative Return"],
        horizontal=False,
    )

# ---------------------------------------------------------------------
# Top row — Control Center
# ---------------------------------------------------------------------
top_left, top_right = st.columns([2, 1], gap="large")

with top_left:
    with st.container(border=True):
        st.markdown("#### Signal Snapshot")

        # --- Line 1: As-of only ---
        st.markdown(
            f"<div style='font-size:14px;color:#6b7280;'>As-of</div>"
            f"<div style='font-size:34px;font-weight:700;margin-bottom:12px;'>"
            f"{asof_date.strftime('%Y-%m-%d')}</div>",
            unsafe_allow_html=True,
        )

        # --- Line 2: Metrics ---
        kpi_row(
            [
                ("LONG_SETUP", int(snap_signal.iloc[0]["n_long_setup"])),
                ("OVEREXTENDED", int(snap_signal.iloc[0]["n_overextended"])),
                ("Avg Core Score", round(float(snap_signal.iloc[0]["avg_core_score"]), 2)),
            ]
        )

        st.caption("Breadth snapshot across MAG7 (canonical core signal only).")
        
with top_right:
    with st.container(border=True):
        macro = snap_macro.iloc[0]
        st.markdown("#### Macro Pulse")

        st.plotly_chart(
            fear_greed_dial(float(macro["fear_greed"]), height=240, show_title=False),
            use_container_width=True,
            config={"displayModeBar": False},
        )
        st.caption(f"Regime: **{macro.get('macro_regime_4', 'unknown')}**")
        
st.divider()

# ---------------------------------------------------------------------
# Row 2 — Trending + Macro Context (FnG shading)
# ---------------------------------------------------------------------
st.subheader("📈 Market Trend vs Macro Context")

start_map = {"3m": 90, "6m": 180, "1y": 365, "2y": 730}
start_date = None
if window != "max":
    start_date = (asof_date - pd.Timedelta(days=start_map[window])).strftime("%Y-%m-%d")

trend_df = load_overview_trending(start_date)
if trend_df.empty:
    st.warning("No trending data available for selected window.")
    st.stop()

trend_df["trade_date"] = pd.to_datetime(trend_df["trade_date"])

# --- Controls for what to show ---
all_equities = sorted([t for t in trend_df["ticker"].unique() if not str(t).startswith("^")])
bench_candidates = [t for t in ["^NDX", "^NDXE"] if t in trend_df["ticker"].unique()]

with st.expander("Chart filters", expanded=False):
    show_equities = st.multiselect(
        "Show tickers",
        options=all_equities,
        default=all_equities,
    )
    show_bench = st.multiselect(
        "Show benchmarks",
        options=bench_candidates,
        default=bench_candidates,
    )
    fng_shading = st.checkbox("Use Fear & Greed as background shading", value=True)
    fng_show_line = st.checkbox("Also show Fear & Greed line (secondary axis)", value=False)

# --- Build figure ---
fig = go.Figure()

# Helper to transform series by mode
def _transform_series(sub: pd.DataFrame) -> pd.Series:
    y = sub["adj_close"].astype(float)

    if price_mode == "Indexed (100)":
        return 100 * y / y.iloc[0]
    if price_mode == "Cumulative Return":
        r = sub["return_1d"].astype(float).fillna(0.0)
        return (1 + r).cumprod()
    return y

# --- (A) FnG background shading bands ---
# Use daily FnG series (unique by date). We'll add vrect bands.
if fng_shading and "fear_greed" in trend_df.columns:
    fg = (
        trend_df.drop_duplicates("trade_date")[["trade_date", "fear_greed"]]
        .sort_values("trade_date")
        .dropna()
    )
    if not fg.empty:
        # map FnG value to regime band
        def _band(x: float) -> str:
            if x < 25: return "extreme_fear"
            if x < 45: return "fear"
            if x < 55: return "neutral"
            if x < 75: return "greed"
            return "extreme_greed"

        fg["band"] = fg["fear_greed"].apply(_band)

        # compress contiguous dates with same band -> fewer shapes
        fg["band_change"] = (fg["band"] != fg["band"].shift(1)).cumsum()
        spans = fg.groupby("band_change").agg(
            band=("band", "first"),
            x0=("trade_date", "min"),
            x1=("trade_date", "max"),
        ).reset_index(drop=True)

        # band colors (light transparency)
        band_fill = {
            "extreme_fear": "rgba(178,34,34,0.10)",
            "fear":         "rgba(255,127,14,0.10)",
            "neutral":      "rgba(211,211,211,0.10)",
            "greed":        "rgba(44,160,44,0.10)",
            "extreme_greed":"rgba(0,100,0,0.10)",
        }

        for _, row in spans.iterrows():
            # extend x1 by 1 day so the band covers the last day visually
            fig.add_vrect(
                x0=row["x0"],
                x1=row["x1"] + pd.Timedelta(days=1),
                fillcolor=band_fill.get(row["band"], "rgba(200,200,200,0.08)"),
                opacity=1.0,
                line_width=0,
                layer="below",
            )

# --- (B) Equity lines ---
for ticker in show_equities:
    sub = trend_df[trend_df["ticker"] == ticker].sort_values("trade_date")
    if sub.empty:
        continue
    y = _transform_series(sub)

    fig.add_trace(
        go.Scatter(
            x=sub["trade_date"],
            y=y,
            name=ticker,
            mode="lines",
            line=dict(width=2),
        )
    )

# --- (C) Benchmark lines ---
for bench in show_bench:
    sub = trend_df[trend_df["ticker"] == bench].sort_values("trade_date")
    if sub.empty:
        continue
    y = _transform_series(sub)

    fig.add_trace(
        go.Scatter(
            x=sub["trade_date"],
            y=y,
            name=bench,
            mode="lines",
            line=dict(width=3, dash="dot"),
        )
    )

# --- (D) Optional FnG line on secondary axis (for debugging / reference) ---
if fng_show_line and "fear_greed" in trend_df.columns:
    fg = (
        trend_df.drop_duplicates("trade_date")[["trade_date", "fear_greed"]]
        .sort_values("trade_date")
        .dropna()
    )
    if not fg.empty:
        fig.add_trace(
            go.Scatter(
                x=fg["trade_date"],
                y=fg["fear_greed"],
                name="Fear & Greed",
                yaxis="y2",
                mode="lines",
                line=dict(width=1),
                opacity=0.35,
            )
        )

fig.update_layout(
    height=440,
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(title=""),
    yaxis=dict(title=price_mode),
    yaxis2=dict(
        title="Fear & Greed",
        overlaying="y",
        side="right",
        range=[0, 100],
        showgrid=False,
        visible=fng_show_line,  # only show the axis if line is enabled
    ),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
)

st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

st.divider()

# ---------------------------------------------------------------------
# Row 3 — Today table
# ---------------------------------------------------------------------
st.subheader("📋 Today Snapshot — Sentiment, TA & Signal")

def highlight_state(val: str) -> str:
    color = S0_SIGNAL_COLORS.get(val, "#FFFFFF")
    return f"background-color: {color}; color: white;"

preferred_cols = [
    "trade_date",
    "ticker",
    "adj_close",
    "return_1d",
    "return_5d",
    "return_20d",
    "article_count",
    "sentiment_mean",
    "finbert_net_ma7",
    "rsi_14",
    "ma_20",
    "ma_50",
    "ma_200",
    "atr_14",
    "vola_20d",
    "core_signal_state",
    "core_score",
    "regime_bucket_10",
    "zscore_bucket_10",
    "fear_greed",
    "macro_regime_4",
]
cols = [c for c in preferred_cols if c in today_df.columns]
table_df = today_df[cols] if cols else today_df

styled = (
    table_df.sort_values("ticker")
    .style
    .applymap(
        highlight_state,
        subset=["core_signal_state"] if "core_signal_state" in table_df.columns else [],
    )
    .format(
        {
            "adj_close": "{:.2f}",
            "return_1d": "{:.2%}",
            "return_5d": "{:.2%}",
            "return_20d": "{:.2%}",
            "vola_20d": "{:.2%}",
            "atr_14": "{:.2f}",
            "rsi_14": "{:.1f}",
            "core_score": "{:.1f}",
            "sentiment_mean": "{:.3f}",
            "finbert_net_ma7": "{:.3f}",
        }
    )
)

st.dataframe(
    styled,
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "ⓘ Orientation and monitoring only. "
    "Use Research pages for validation and outcome analysis."
)
