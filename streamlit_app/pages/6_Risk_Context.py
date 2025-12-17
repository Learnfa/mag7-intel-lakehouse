import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.data_loaders import (
    load_risk_dashboard_latest,
    load_macro_risk_latest,
    load_macro_risk_history,
)
from components.banners import production_truth_banner
from components.freshness import data_freshness_panel
from components.tables import styled_signal_table 

st.set_page_config(
    page_title="Risk Context | MAG7 Intel",
    page_icon="⚠️",
    layout="wide",
)

st.title("⚠️ Risk Context")
st.caption("Observational risk overlays • No gating • No strategy logic")

production_truth_banner()

# ---------------------------------------------------------------------
# Load latest snapshots
# ---------------------------------------------------------------------
with st.spinner("Loading latest equity risk snapshot…"):
    risk_latest = load_risk_dashboard_latest()

with st.spinner("Loading latest macro snapshot…"):
    macro_latest = load_macro_risk_latest()

if risk_latest.empty:
    st.error("No rows returned from `risk_dashboard`.")
    st.stop()

if "ticker" not in risk_latest.columns:
    st.error("`risk_dashboard` must include a `ticker` column.")
    st.stop()
    
tickers = sorted(risk_latest["ticker"].dropna().unique().tolist()) if "ticker" in risk_latest.columns else []
if not tickers:
    st.error("`risk_dashboard` must include a `ticker` column.")
    st.stop()

# ---------------------------------------------------------------------
# Data freshness: use risk_dashboard asof_date (new mart field)
# ---------------------------------------------------------------------
asof_date = None
if "asof_date" in risk_latest.columns:
    asof_date = pd.to_datetime(risk_latest["asof_date"], errors="coerce").max()

# Fallback to macro if risk mart doesn't have asof_date for any reason
if asof_date is None and not macro_latest.empty and "trade_date" in macro_latest.columns:
    asof_date = pd.to_datetime(macro_latest["trade_date"], errors="coerce").max()

data_freshness_panel(
    asof_date=asof_date,
    sources=[
        "mag7_intel_mart.risk_dashboard",
        "mag7_intel_mart.macro_risk_dashboard",
    ],
    location="sidebar",
)

# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")
    selected_ticker = st.selectbox("Ticker", options=tickers, index=0)
    show_macro_overlay = st.checkbox("Show macro timeline", value=True)

# ---------------------------------------------------------------------
# Latest snapshot table (risk_dashboard is a summary)
# ---------------------------------------------------------------------
st.subheader("📌 Latest Equity Risk Snapshot")
st.caption("`risk_dashboard` is an aggregated summary (not a daily time series).")

# Put date context columns first if present
front_cols = [c for c in ["ticker", "asof_date", "window_start_date", "window_end_date"] if c in risk_latest.columns]
other_cols = [c for c in risk_latest.columns if c not in front_cols]
risk_show = risk_latest[front_cols + other_cols].copy()

# Optional: use shared table component for consistent formatting (no coloring)
dummy_col = "__row_class"
risk_show[dummy_col] = "ROW"

styled_signal_table(
    risk_show,
    signal_col=dummy_col,
    color_map={"ROW": "#111827"},
)
st.divider()

# ---------------------------------------------------------------------
# Cross-Sectional Risk Snapshot
# ---------------------------------------------------------------------
st.subheader("📊 Cross-Sectional Risk Snapshot")
st.caption(
    "Latest cross-sectional comparison across tickers. "
    "Sortable, descriptive, no strategy logic."
)

preferred_cols = [
    "ticker",
    "annualized_return",
    "annualized_volatility",
    "annualized_downside_volatility",
    "max_drawdown",
    "ndx_tracking_error",
    "ndxe_tracking_error",
    "ndx_excess_negative_rate",
    "ndxe_excess_negative_rate",
    "pct_time_value_regimes",
    "pct_time_mid_regimes",
    "pct_time_momentum_regimes",
    "pct_time_deep_value",
    "pct_time_value_setup",
    "pct_time_momentum",
    "pct_time_overextended",
]

available_cols = [c for c in preferred_cols if c in risk_latest.columns]

if len(available_cols) < 2:
    st.info(
        "Not enough known risk columns found to render cross-sectional snapshot. "
        "Ensure `risk_dashboard` mart includes metrics like volatility or drawdown."
    )
else:
    # Sidebar controls for this section only
    with st.sidebar:
        st.markdown("### Cross-Section Controls")
        sort_col = st.selectbox(
            "Sort by",
            options=[c for c in available_cols if c != "ticker"],
            index=0,
        )
        sort_ascending = st.checkbox("Ascending", value=False)

    snapshot_df = (
        risk_latest[available_cols]
        .copy()
        .sort_values(sort_col, ascending=sort_ascending)
    )

    snap_show = snapshot_df.copy()
    snap_show[dummy_col] = "ROW"
    styled_signal_table(
        snap_show,
        signal_col=dummy_col,
        color_map={"ROW": "#111827"},
    )

st.caption(
    "ⓘ This table is a **snapshot view** (latest date only). "
    "Metrics are descriptive and should not be interpreted as signals."
)

st.divider()

# ---------------------------------------------------------------------
# Selected ticker: show a compact “profile” (since no time series here)
# ---------------------------------------------------------------------
st.subheader(f"🧾 Risk Profile — {selected_ticker}")
row = risk_latest[risk_latest["ticker"] == selected_ticker].copy()

if row.empty:
    st.warning(f"No row found for {selected_ticker} in `risk_dashboard`.")
else:
    # Render as key-value style table
    profile = row.iloc[0].to_dict()

    # Keep this concise: show important fields first
    show_keys = [k for k in preferred_cols if k in profile] + [k for k in ["asof_date", "window_start_date", "window_end_date"] if k in profile]
    show_keys = list(dict.fromkeys(show_keys))  # de-dupe preserving order

    profile_df = pd.DataFrame(
        {"metric": show_keys, "value": [profile[k] for k in show_keys]}
    )

    st.dataframe(profile_df, use_container_width=True, hide_index=True)

st.divider()

# ---------------------------------------------------------------------
# Macro timeline (optional)
# ---------------------------------------------------------------------
if show_macro_overlay:
    st.subheader("🌍 Macro Timeline (Observational)")

    with st.spinner("Loading macro history…"):
        macro_hist = load_macro_risk_history()

    if macro_hist.empty:
        st.info("No rows returned from `macro_risk_dashboard` history.")
    else:
        macro_hist = macro_hist.copy()
        if "trade_date" in macro_hist.columns:
            macro_hist["trade_date"] = pd.to_datetime(macro_hist["trade_date"])
            macro_hist = macro_hist.sort_values("trade_date")

        macro_cols_candidates = [
            "fear_greed", "fear_greed_score",
            "macro_risk_off_score_20d", "macro_risk_off_score",
            "gdelt_tone", "news_sentiment",
        ]
        macro_cols = [c for c in macro_cols_candidates if c in macro_hist.columns]

        if not macro_cols or "trade_date" not in macro_hist.columns:
            st.dataframe(macro_latest, use_container_width=True, hide_index=True)
            st.caption("ⓘ Macro mart loaded, but no known macro columns were detected for charting.")
        else:
            macro_metric = st.selectbox("Macro metric", options=macro_cols, index=0)

            fig = go.Figure(
                data=[
                    go.Scatter(
                        x=macro_hist["trade_date"],
                        y=pd.to_numeric(macro_hist[macro_metric], errors="coerce"),
                        mode="lines",
                        name=macro_metric,
                        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Value: %{y}<extra></extra>",
                    )
                ]
            )
            fig.update_layout(
                height=380,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(title=""),
                yaxis=dict(title=macro_metric),
            )
            st.plotly_chart(fig, use_container_width=True)

st.caption("ⓘ Risk Context is observational. It does not modify `signal_core` (no gating).")