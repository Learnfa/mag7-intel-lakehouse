import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from components.banners import production_truth_banner
from components.freshness import data_freshness_panel
from components.metrics import kpi_row

from utils.data_loaders import (
    load_price_macro,
    load_available_tickers,
    load_date_bounds,
)

st.set_page_config(
    page_title="Stock + Macro | MAG7 Intel",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Stock Analysis + Macro Sentiment")
st.caption("Price, moving averages, Fear & Greed shading, and indicator ↔ forward return correlations.")

production_truth_banner()

# ----------------------------
# Sidebar controls
# ----------------------------
with st.sidebar:
    st.subheader("Filters")

    all_tickers = load_available_tickers()
    if not all_tickers:
        st.error("No tickers found in core price table.")
        st.stop()

    default_tickers = all_tickers[:1]
    tickers = st.multiselect("Ticker(s)", all_tickers, default=default_tickers)

    min_date_str, max_date_str = load_date_bounds()
    min_date = pd.to_datetime(min_date_str).date()
    max_date = pd.to_datetime(max_date_str).date()

    st.markdown("---")
    st.subheader("Time Period")

    preset = st.selectbox(
        "Quick range",
        ["Custom", "1Y", "6M", "3M", "1M", "YTD"],
        index=1,
    )

    if preset != "Custom":
        end_date = max_date
        if preset == "YTD":
            start_date = pd.Timestamp(end_date).replace(month=1, day=1).date()
        else:
            months = {"1M": 1, "3M": 3, "6M": 6, "1Y": 12}[preset]
            start_date = (pd.Timestamp(end_date) - pd.DateOffset(months=months)).date()
            start_date = max(start_date, min_date)
    else:
        start_date = st.date_input("Start", value=min_date, min_value=min_date, max_value=max_date)
        end_date = st.date_input("End", value=max_date, min_value=min_date, max_value=max_date)

    if start_date > end_date:
        st.error("Start date must be <= end date.")
        st.stop()

# ----------------------------
# Load data
# ----------------------------
with st.spinner("Loading from BigQuery…"):
    df = load_price_macro(
        tickers=tickers,
        start_date=str(start_date),
        end_date=str(end_date),
    )

if df.empty:
    st.warning("No data for your selection.")
    st.stop()

data_freshness_panel(
    asof_date=df["trade_date"].max() if (not df.empty and "trade_date" in df.columns) else None,
    sources=[
        "mag7_intel_core.fact_prices",
        "mag7_intel_core.fact_macro_sentiment_daily",
    ],
)

# If multiple tickers selected, let user pick one for the main chart.
if len(tickers) > 1:
    selected_ticker = st.selectbox("Primary ticker for charts", tickers, index=0)
else:
    selected_ticker = tickers[0]

df_t = df[df["ticker"] == selected_ticker].copy()
df_t["fear_greed"] = df_t["fear_greed"].fillna(50)  # neutral fill for viz

# ----------------------------
# KPIs row
# ----------------------------
latest = df_t.sort_values("trade_date").tail(1).iloc[0]
kpis = [
    ("As of", latest["trade_date"].strftime("%Y-%m-%d")),
    ("Adj Close", f"{latest['adj_close']:.2f}"),
    ("Volume", f"{int(latest['volume']):,}" if pd.notna(latest["volume"]) else "—"),
    ("Fear/Greed", f"{int(latest['fear_greed']):d}" if pd.notna(latest["fear_greed"]) else "—"),
]
kpi_row(kpis)

# ----------------------------
# Price chart with Fear/Greed shading background
# ----------------------------
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.06,
    row_heights=[0.72, 0.28],
)

# Background shading bands (simple 4-zone)
# 0-25 Extreme Fear, 25-50 Fear, 50-75 Greed, 75-100 Extreme Greed
bands = [
    (0, 25, "rgba(220, 20, 60, 0.10)"),
    (25, 50, "rgba(255, 140, 0, 0.08)"),
    (50, 75, "rgba(50, 205, 50, 0.07)"),
    (75, 100, "rgba(0, 128, 0, 0.08)"),
]

# Create a normalized (0-100) series on secondary hidden axis via filled area trick:
# We draw filled areas using scatter with y in [band_low, band_high] and fill='tonexty' style.
x = df_t["trade_date"]

# Start with baseline at 0
fig.add_trace(
    go.Scatter(
        x=x, y=np.zeros(len(df_t)),
        mode="lines",
        line=dict(width=0),
        hoverinfo="skip",
        showlegend=False,
        name="baseline",
    ),
    row=1, col=1,
)

# For each band, draw y=band_high and fill to previous, but only where fear_greed within band
fg = df_t["fear_greed"].to_numpy()

prev = np.zeros(len(df_t))
for low, high, color in bands:
    y = np.where((fg >= low) & (fg < high), high, prev)
    fig.add_trace(
        go.Scatter(
            x=x, y=y,
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor=color,
            hoverinfo="skip",
            showlegend=False,
        ),
        row=1, col=1,
    )
    prev = y

hover_template = (
    "<b>Date:</b> %{x|%Y-%m-%d}<br>"
    "<b>Adj Close:</b> %{y:.2f}<br>"
    "<b>Open/High/Low:</b> %{customdata[0]:.2f} / %{customdata[1]:.2f} / %{customdata[2]:.2f}<br>"
    "<b>Volume:</b> %{customdata[3]:,.0f}<br>"
    "<b>Fear/Greed:</b> %{customdata[4]:.0f}"
    "<extra></extra>"
)
custom_data = df_t[["open", "high", "low", "volume", "fear_greed"]].values

# Price line
fig.add_trace(
    go.Scatter(
        x=df_t["trade_date"],
        y=df_t["adj_close"],
        mode="lines",
        name="Adj Close",
        customdata=custom_data,
        hovertemplate=hover_template,
    ),
    row=1, col=1,
)

# Moving averages (only if present)
for ma in ["ma_20", "ma_50", "ma_200"]:
    if ma in df_t.columns and df_t[ma].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df_t["trade_date"],
                y=df_t[ma],
                mode="lines",
                name=ma.upper(),
                line=dict(width=1),
            ),
            row=1, col=1,
        )

# Volume bars (neutral)
fig.add_trace(
    go.Bar(
        x=df_t["trade_date"],
        y=df_t["volume"],
        name="Volume",
        opacity=0.85,
    ),
    row=2, col=1,
)

fig.update_layout(
    height=650,
    template="plotly_dark",
    title=f"{selected_ticker} • Price + Volume (Fear/Greed shaded background)",
    hovermode="x unified",
    legend=dict(orientation="h", y=1.02, x=0, xanchor="left"),
    margin=dict(l=10, r=10, t=60, b=10),
)
fig.update_xaxes(rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# Correlation section
# ----------------------------
st.markdown("---")
st.subheader("🔍 Indicator ↔ Forward Return Analysis")

tab1, tab2 = st.tabs(["Indicator vs Return Scatter", "Heatmap (Returns vs Indicators)"])

return_vars = ["fwd_return_1d", "fwd_return_5d", "fwd_return_10d", "fwd_return_20d"]
indicator_vars = [
    "fear_greed",
    "mkt_sp500", "mkt_sp125",
    "stock_strength", "stock_breadth",
    "put_call", "volatility", "volatility_50",
    "safe_haven", "junk_bonds",
]
available_indicators = [c for c in indicator_vars if c in df_t.columns]

with tab1:
    c1, c2 = st.columns([1, 2])

    with c1:
        horizon_labels = {
            "fwd_return_1d": "1D",
            "fwd_return_5d": "5D",
            "fwd_return_10d": "10D",
            "fwd_return_20d": "20D",
        }
        y_col = st.selectbox("Return horizon (Y)", return_vars, format_func=lambda x: horizon_labels[x])
        x_col = st.selectbox("Indicator (X)", available_indicators, index=0)
        add_trend = st.toggle("Add simple trendline", value=True)

    with c2:
        scatter_df = df_t[[x_col, y_col, "volume"]].dropna()
        if scatter_df.empty:
            st.warning("Not enough data points after dropping NaNs.")
        else:
            fig_scatter = px.scatter(
                scatter_df,
                x=x_col,
                y=y_col,
                size="volume",
                hover_data=["volume"],
                labels={x_col: x_col.replace("_", " ").title(), y_col: "Forward Return"},
                title=f"{selected_ticker}: {x_col} vs {horizon_labels[y_col]} forward return",
            )
            fig_scatter.update_layout(template="plotly_dark", height=520)
            fig_scatter.update_yaxes(tickformat=".1%")

            if add_trend and len(scatter_df) >= 3:
                # simple least-squares line (no statsmodels dependency)
                x_vals = scatter_df[x_col].astype(float).to_numpy()
                y_vals = scatter_df[y_col].astype(float).to_numpy()
                m, b = np.polyfit(x_vals, y_vals, 1)
                x_line = np.linspace(np.nanmin(x_vals), np.nanmax(x_vals), 50)
                y_line = m * x_line + b
                fig_scatter.add_trace(go.Scatter(x=x_line, y=y_line, mode="lines", name="Trend"))

            st.plotly_chart(fig_scatter, use_container_width=True)

with tab2:
    st.caption("Correlation computed after dropping rows with missing values.")
    avail_returns = [c for c in return_vars if c in df_t.columns]
    if not available_indicators or not avail_returns:
        st.warning("Missing return or indicator columns.")
    else:
        corr_df = df_t[avail_returns + available_indicators].dropna()
        if corr_df.empty:
            st.warning("No rows left after dropping NaNs.")
        else:
            corr = corr_df.corr(numeric_only=True).loc[avail_returns, available_indicators]
            fig_hm = px.imshow(
                corr,
                text_auto=".2f",
                aspect="auto",
                zmin=-1, zmax=1,
                labels=dict(x="Indicator", y="Forward Return", color="Corr"),
            )
            fig_hm.update_layout(template="plotly_dark", height=520)
            st.plotly_chart(fig_hm, use_container_width=True)
