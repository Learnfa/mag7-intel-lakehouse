import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.data_loaders import load_regime_summary, load_s0_core_dates
from components.banners import production_truth_banner
from components.freshness import data_freshness_panel
from components.tables import styled_signal_table
from components.date_glider import date_glider


st.set_page_config(
    page_title="Regimes | MAG7 Intel",
    page_icon="🧭",
    layout="wide",
)

st.title("🧭 Regimes")
st.caption("Distribution & regime diagnostics • No performance claims unless explicitly present in the mart")

production_truth_banner()

with st.spinner("Loading regime summary…"):
    df = load_regime_summary()

if df.empty:
    st.error("No rows returned from `regime_summary`.")
    st.stop()


# ---------------------------------------------------------------------
# Controls (sidebar): date glider + filters
# ---------------------------------------------------------------------with st.sidebar:
with st.sidebar:
    st.markdown("## Controls")
    
    # --- As-at Date (shared glider) ---
    with st.spinner("Loading available dates…"):
        dates_list = load_s0_core_dates()

    # HARD sanitize (important)
    if isinstance(dates_list, pd.DataFrame):
        dates_list = dates_list["trade_date"].tolist()
    elif isinstance(dates_list, pd.Series):
        dates_list = dates_list.tolist()
    else:
        dates_list = list(dates_list)

    if not dates_list:
        st.error("No trade dates available.")
        st.stop()

    st.sidebar.caption(f"{len(dates_list)} trading dates available")

    asof_date = date_glider(
        dates_list,
        label="As-at Date",
        key="regimes_date_glider",
        formatter=lambda d: d.strftime("%Y-%m-%d"),
    )
    
# ---------------------------------------------------------------------
# Data freshness (Regime summary is aggregate → anchor to signal_core)
# ---------------------------------------------------------------------
    data_freshness_panel(
        asof_date=asof_date,
        sources=[
            "_mart.regime_summary",
            "_mart.signal_core",
        ],
        location="sidebar",
    )

    # --- Ticker filter ---
    tickers = (
        sorted(df["ticker"].dropna().unique().tolist())
        if "ticker" in df.columns
        else []
    )

    selected_tickers = st.multiselect(
        label="Tickers",
        options=tickers,
        default=tickers,
        label_visibility="collapsed",
        key="regimes_tickers",
    )
    
    # Defensive default
    if "selected_tickers" not in locals():
        selected_tickers = []
    
view = df.copy()
if "ticker" in view.columns and selected_tickers:
    view = view[view["ticker"].isin(selected_tickers)]

st.subheader("📋 Regime Summary Table")
# Use shared table component for consistent float formatting
table_df = view.copy()

# Provide a dummy signal column so the component can run without changing colors
dummy_col = "__row_class"
table_df[dummy_col] = "ROW"

styled_signal_table(
    table_df,
    signal_col=dummy_col,
    color_map={"ROW": "#111827"},  # dark neutral (won't distract)
)

st.divider()

# Try to plot regime distribution if columns exist
st.subheader("📊 Regime Distribution")

needed_cols = {"ticker", "regime_bucket_10"}
has_counts = any(c in view.columns for c in ["n_obs", "count", "obs", "num_obs"])
has_pct = any(c in view.columns for c in ["pct_obs", "pct", "share"])

if not needed_cols.issubset(set(view.columns)):
    st.info("`regime_summary` does not contain the minimum columns to plot distribution (needs `ticker`, `regime_bucket_10`).")
    st.stop()

# --------------------------------------------------
# Choose or derive distribution metric
# --------------------------------------------------
if "pct_obs" in view.columns:
    metric_col = "pct_obs"
    y_title = "Share of Observations"

elif "pct" in view.columns:
    metric_col = "pct"
    y_title = "Share of Observations"

elif "n_obs" in view.columns:
    metric_col = "n_obs"
    y_title = "Count of Observations"

elif "count" in view.columns:
    metric_col = "count"
    y_title = "Count of Observations"

else:
    # ---- FALLBACK: derive counts from rows ----
    st.caption("ⓘ No precomputed counts found. Deriving distribution from rows.")

    group_cols = ["ticker", "regime_bucket_10"]
    dist = (
        view
        .dropna(subset=["regime_bucket_10"])
        .groupby(group_cols)
        .size()
        .reset_index(name="derived_count")
    )

    view = dist
    metric_col = "derived_count"
    y_title = "Count of Observations"
    
    
# Plot: one chart per ticker selector (clean & readable)
if "ticker" in view.columns and selected_tickers:
    ticker_to_plot = st.selectbox("Ticker to plot", options=selected_tickers, index=0)
    plot_df = view[view["ticker"] == ticker_to_plot].copy()
else:
    ticker_to_plot = None
    plot_df = view.copy()

plot_df["regime_bucket_10"] = pd.to_numeric(plot_df["regime_bucket_10"], errors="coerce").astype("Int64")
plot_df = plot_df.dropna(subset=["regime_bucket_10"]).sort_values("regime_bucket_10")

fig = go.Figure(
    data=[
        go.Bar(
            x=plot_df["regime_bucket_10"].astype(int),
            y=pd.to_numeric(plot_df[metric_col], errors="coerce"),
            hovertemplate="Bucket: %{x}<br>Value: %{y}<extra></extra>",
        )
    ]
)
fig.update_layout(
    height=420,
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(title="Regime Bucket (1=cheap → 10=expensive)", dtick=1),
    yaxis=dict(title=y_title),
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ This page is descriptive. If `regime_summary` includes forward-return columns, treat them as research context unless explicitly validated."
)
