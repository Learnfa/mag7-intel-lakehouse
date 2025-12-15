import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.data_loaders import      \
    load_s0_core_latest,  \
    load_s0_core_history, \
    load_price_corridor_history
from components.banners import production_truth_banner
from components.metrics import kpi_row
from components.freshness import data_freshness_panel
from components.tables import styled_signal_table
from utils.constants import S0_SIGNAL_COLORS


st.set_page_config(
    page_title="Ticker Deep Dive | MAG7 Intel",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Ticker Deep Dive")
st.caption("Explainable context • Price corridor • Signal markers • No performance")

production_truth_banner()


# ---------------------------------------------------------------------
# Load latest snapshot (for selector defaults + KPIs)
# ---------------------------------------------------------------------
with st.spinner("Loading latest signal snapshot…"):
    latest_df = load_s0_core_latest()

if latest_df.empty:
    st.error("No data found in `signal_core`.")
    st.stop()

latest_df = latest_df.copy()
latest_df["trade_date"] = pd.to_datetime(latest_df["trade_date"])
asof_date = latest_df["trade_date"].max()
tickers = sorted(latest_df["ticker"].unique())

data_freshness_panel(
    asof_date=asof_date,
    sources=["mag7_intel_mart.signal_core", "mag7_intel_core.fact_prices"],
    location="sidebar",
)

# Sidebar controls
with st.sidebar:
    st.markdown("## Controls")
    selected_ticker = st.selectbox("Ticker", options=tickers, index=0)
    show_corridor = st.checkbox("Show price corridor (200d min/max)", value=True)
    show_markers = st.checkbox("Show signal markers", value=True)
    show_locator = st.checkbox("Show regime × z-score locator", value=True)


# ---------------------------------------------------------------------
# Load ticker history
# ---------------------------------------------------------------------
with st.spinner(f"Loading signal history for {selected_ticker}…"):
    sig_hist = load_s0_core_history(selected_ticker)

if sig_hist.empty:
    st.warning(f"No signal history found for {selected_ticker}")
    st.stop()

sig_hist = sig_hist.copy()
sig_hist["trade_date"] = pd.to_datetime(sig_hist["trade_date"])
sig_hist = sig_hist.sort_values("trade_date")

with st.spinner(f"Loading price corridor for {selected_ticker}…"):
    px = load_price_corridor_history(selected_ticker)

if px.empty:
    st.warning(f"No price history found for {selected_ticker}")
    st.stop()

px = px.copy()
px["trade_date"] = pd.to_datetime(px["trade_date"])
px = px.sort_values("trade_date")


# ---------------------------------------------------------------------
# Align date ranges (intersection)
# ---------------------------------------------------------------------
min_date = max(sig_hist["trade_date"].min(), px["trade_date"].min())
max_date = min(sig_hist["trade_date"].max(), px["trade_date"].max())

with st.sidebar:
    st.subheader("Date Range")
    date_range = st.date_input(
        "Select range",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])

sig_hist = sig_hist[(sig_hist["trade_date"] >= start_date) & (sig_hist["trade_date"] <= end_date)]
px = px[(px["trade_date"] >= start_date) & (px["trade_date"] <= end_date)]


# ---------------------------------------------------------------------
# KPI row (current state)
# ---------------------------------------------------------------------
current = latest_df[latest_df["ticker"] == selected_ticker]
if current.empty:
    # fallback to last row from history
    current_row = sig_hist.iloc[-1]
else:
    current_row = current.iloc[0]

kpi_row(
    [
        ("As-of Date", asof_date.strftime("%Y-%m-%d")),
        ("Ticker", selected_ticker),
        ("Current State", current_row["core_signal_state"]),
        ("Regime Bucket", int(current_row["regime_bucket_10"])),
        ("Z-Score Bucket", int(current_row["zscore_bucket_10"])),
        ("Core Score", float(current_row["core_score"])),
    ]
)

st.divider()


# ---------------------------------------------------------------------
# Chart 1: Price + Corridor + Signal markers
# ---------------------------------------------------------------------
st.subheader("📉 Price Context (with 200-day corridor)")

fig = go.Figure()

# Price line
fig.add_trace(
    go.Scatter(
        x=px["trade_date"],
        y=px["adj_close"],
        mode="lines",
        name="Close",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Close: %{y:.2f}<extra></extra>",
    )
)

# Corridor band (optional)
if show_corridor:
    # upper band
    fig.add_trace(
        go.Scatter(
            x=px["trade_date"],
            y=px["roll_max_200d"],
            mode="lines",
            name="200d Max",
            line=dict(width=1),
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>200d Max: %{y:.2f}<extra></extra>",
            opacity=0.35,
        )
    )

    # lower band (fill to previous trace)
    fig.add_trace(
        go.Scatter(
            x=px["trade_date"],
            y=px["roll_min_200d"],
            mode="lines",
            name="200d Min",
            line=dict(width=1),
            fill="tonexty",
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>200d Min: %{y:.2f}<extra></extra>",
            opacity=0.35,
        )
    )

# Signal markers (optional)
if show_markers:
    for state in ["LONG_SETUP", "OVEREXTENDED"]:
        sub = sig_hist[sig_hist["core_signal_state"] == state]
        if not sub.empty:
            # join to close for y position
            sub2 = sub.merge(px[["trade_date", "adj_close"]], on="trade_date", how="left")
            fig.add_trace(
                go.Scatter(
                    x=sub2["trade_date"],
                    y=sub2["adj_close"],
                    mode="markers",
                    name=f"{state} marker",
                    marker=dict(
                        size=8,
                        color=S0_SIGNAL_COLORS.get(state, "#999999"),
                        symbol="circle",
                        line=dict(width=0),
                    ),
                    hovertemplate=(
                        "<b>%{x|%Y-%m-%d}</b><br>"
                        f"State: {state}<br>"
                        "Close: %{y:.2f}<br>"
                        "<extra></extra>"
                    ),
                )
            )

fig.update_layout(
    height=520,
    margin=dict(l=10, r=10, t=10, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    xaxis=dict(title="", showgrid=True),
    yaxis=dict(title="Price", showgrid=True),
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ Corridor uses rolling min/max over the last 200 trading rows (approx 200 trading days). "
    "Markers show when `signal_core` entered LONG_SETUP or OVEREXTENDED."
)

st.divider()


# ---------------------------------------------------------------------
# Chart 2: Regime × Z-score Locator (10×10 grid)
# ---------------------------------------------------------------------
if show_locator:
    st.subheader("🧭 Regime × Z-score Locator (Today)")

    rb = int(current_row["regime_bucket_10"])
    zb = int(current_row["zscore_bucket_10"])

    # Build an empty 10x10 grid for visualization (no research returns)
    grid = [[0 for _ in range(10)] for __ in range(10)]

    heat = go.Heatmap(
        z=grid,
        x=list(range(1, 11)),  # Regime bucket
        y=list(range(1, 11)),  # Z bucket
        showscale=False,
        hoverinfo="skip",
        opacity=0.15,
    )

    dot = go.Scatter(
        x=[rb],
        y=[zb],
        mode="markers+text",
        marker=dict(size=16, color=S0_SIGNAL_COLORS.get(current_row["core_signal_state"], "#2563EB")),
        text=["●"],
        textposition="middle center",
        hovertemplate=(
            f"<b>{selected_ticker}</b><br>"
            f"Regime bucket: {rb}<br>"
            f"Z-score bucket: {zb}<br>"
            f"State: {current_row['core_signal_state']}<br>"
            "<extra></extra>"
        ),
        showlegend=False,
    )

    fig2 = go.Figure(data=[heat, dot])
    fig2.update_layout(
        height=420,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title="Regime Bucket (1=cheapest → 10=most expensive)", dtick=1),
        yaxis=dict(title="Z-score Bucket (1=most oversold → 10=most overbought)", dtick=1),
    )

    st.plotly_chart(fig2, use_container_width=True)

    st.caption(
        "ⓘ This locator is a **position view only** (no forward-return coloring). "
        "Research heatmaps belong on the Research pages."
    )

st.divider()


# ---------------------------------------------------------------------
# Inspectable recent rows (debug-friendly)
# ---------------------------------------------------------------------
st.subheader("🔎 Recent Rows")
st.caption("Signal + price context for the last 60 rows in the selected date range.")

merged = sig_hist.merge(px, on=["trade_date"], how="left")

# Ensure ticker column exists (deep dive is single-ticker, so safe to inject)
if "ticker" not in merged.columns:
    merged = merged.copy()
    merged["ticker"] = selected_ticker

recent = merged.sort_values("trade_date", ascending=False).head(60)

def highlight_state(val: str) -> str:
    color = S0_SIGNAL_COLORS.get(val, "#FFFFFF")
    return f"background-color: {color}; color: white;"

# Select only columns that exist (extra defensive)
cols = [
    "trade_date",
    "ticker",
    "adj_close",
    "roll_min_200d",
    "roll_max_200d",
    "regime_bucket_10",
    "zscore_bucket_10",
    "price_pos_200d",
    "price_zscore_20d",
    "core_signal_state",
    "core_score",
]
existing_cols = [c for c in cols if c in recent.columns]

styled_signal_table(
    recent[existing_cols],
    signal_col="core_signal_state",
    color_map=S0_SIGNAL_COLORS,
)

st.caption(
    "ⓘ This page uses only `signal_core` + `fact_prices` for explainability. "
    "No forward returns, equity curves, or strategy performance are shown."
)
