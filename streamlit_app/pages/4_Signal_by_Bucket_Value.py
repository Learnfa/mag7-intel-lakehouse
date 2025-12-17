import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.content_loaders import load_markdown
from utils.data_loaders import load_s0_core_latest, load_s0_core_history
from components.banners import production_truth_banner
from components.metrics import kpi_row
from components.freshness import data_freshness_panel
from utils.constants import S0_SIGNAL_COLORS

st.set_page_config(
    page_title="S0 Core Signal | Bucket Value",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 S0 Core Signal | Bucket Value")
st.caption("Canonical signal monitoring • No performance • No research tables")

production_truth_banner()

# ---------------------------------------------------------------------
# Load latest snapshot (for selector defaults + KPI)
# ---------------------------------------------------------------------
with st.spinner("Loading latest core signal snapshot…"):
    latest_df = load_s0_core_latest()

if latest_df.empty:
    st.error("No data found in `mart.s0_core_value`.")
    st.stop()

asof_date = pd.to_datetime(latest_df["trade_date"]).max()
tickers = sorted(latest_df["ticker"].unique())

data_freshness_panel(
    asof_date=asof_date,
    sources=["mag7_intel_mart.signal_core"],
    location="sidebar",
)

# Sidebar controls
with st.sidebar:
    st.markdown("## Controls")

    selected_ticker = st.selectbox("Ticker", options=tickers, index=0)

    show_state_timeline = st.checkbox("Show state timeline", value=True)
    show_persistence = st.checkbox("Show persistence (LONG_SETUP streak)", value=True)
    show_distribution = st.checkbox("Show state distribution", value=True)

# ---------------------------------------------------------------------
# Load history for selected ticker
# ---------------------------------------------------------------------
with st.spinner(f"Loading signal history for {selected_ticker}…"):
    hist = load_s0_core_history(selected_ticker)

if hist.empty:
    st.warning(f"No history found for ticker: {selected_ticker}")
    st.stop()

hist = hist.copy()
hist["trade_date"] = pd.to_datetime(hist["trade_date"])
hist = hist.sort_values("trade_date")

# ---------------------------------------------------------------------
# KPI Row (ticker-level)
# ---------------------------------------------------------------------
n_days = len(hist)
n_long = (hist["core_signal_state"] == "LONG_SETUP").sum()
n_over = (hist["core_signal_state"] == "OVEREXTENDED").sum()
n_neutral = (hist["core_signal_state"] == "NEUTRAL").sum()

latest_row = hist.iloc[-1]

kpi_row(
    [
        ("As-of Date", asof_date.strftime("%Y-%m-%d")),
        ("Ticker", selected_ticker),
        ("Current State", latest_row["core_signal_state"]),
        ("Regime Bucket", int(latest_row["regime_bucket_10"])),
        ("Z-Score Bucket", int(latest_row["zscore_bucket_10"])),
        ("Core Score", float(latest_row["core_score"])),
    ]
)

st.divider()


# ---------------------------------------------------------------------
# Explanation (loaded from file)
# ---------------------------------------------------------------------
load_markdown(
    "contents/s0_bucket_value.md",
    expanded=False,
    title="What does this page show?"
)

# ---------------------------------------------------------------------
# Plotly helpers
# ---------------------------------------------------------------------
STATE_Y = {"OVEREXTENDED": 2, "NEUTRAL": 1, "LONG_SETUP": 0}
Y_STATE_LABELS = {0: "LONG_SETUP", 1: "NEUTRAL", 2: "OVEREXTENDED"}

def _state_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Categorical state timeline shown as colored markers on 3 discrete levels.
    """
    fig = go.Figure()

    for state in ["LONG_SETUP", "NEUTRAL", "OVEREXTENDED"]:
        sub = df[df["core_signal_state"] == state]
        fig.add_trace(
            go.Scatter(
                x=sub["trade_date"],
                y=[STATE_Y[state]] * len(sub),
                mode="markers",
                name=state,
                marker=dict(size=7, color=S0_SIGNAL_COLORS.get(state, "#999999")),
                customdata=sub[["regime_bucket_10", "zscore_bucket_10", "price_pos_200d", "price_zscore_20d", "core_score"]],
                hovertemplate=(
                    "<b>%{x|%Y-%m-%d}</b><br>"
                    f"State: {state}<br>"
                    "Regime bucket: %{customdata[0]}<br>"
                    "Z bucket: %{customdata[1]}<br>"
                    "Pos(200d): %{customdata[2]:.4f}<br>"
                    "Z(20d): %{customdata[3]:.4f}<br>"
                    "Core score: %{customdata[4]:.2f}<br>"
                    "<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(title="", showgrid=True),
        yaxis=dict(
            title="",
            tickmode="array",
            tickvals=list(Y_STATE_LABELS.keys()),
            ticktext=[Y_STATE_LABELS[v] for v in Y_STATE_LABELS.keys()],
            autorange="reversed",  # LONG_SETUP at top visually
            showgrid=False,
        ),
    )
    return fig


def _streak_line(df: pd.DataFrame) -> go.Figure:
    """
    LONG_SETUP persistence streak length over time.
    """
    streak = []
    current = 0
    for s in df["core_signal_state"].tolist():
        if s == "LONG_SETUP":
            current += 1
        else:
            current = 0
        streak.append(current)

    tmp = df.copy()
    tmp["long_streak_days"] = streak

    fig = go.Figure(
        data=[
            go.Scatter(
                x=tmp["trade_date"],
                y=tmp["long_streak_days"],
                mode="lines",
                name="LONG_SETUP streak (days)",
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Streak: %{y} days<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title="", showgrid=True),
        yaxis=dict(title="Days", rangemode="tozero"),
    )
    return fig


# ---------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------
if show_state_timeline:
    st.subheader("🧭 Signal State Timeline")
    st.plotly_chart(_state_scatter(hist), use_container_width=True)

if show_persistence:
    st.subheader("⏳ LONG_SETUP Persistence")
    st.plotly_chart(_streak_line(hist), use_container_width=True)
    st.caption("Shows how many consecutive days the ticker has remained in LONG_SETUP.")

if show_distribution:
    st.subheader("📊 State Distribution")

    dist = (
        hist["core_signal_state"]
        .value_counts()
        .reindex(["LONG_SETUP", "NEUTRAL", "OVEREXTENDED"])
        .fillna(0)
        .astype(int)
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric("Total Days", n_days)
        st.metric("LONG_SETUP Days", n_long)
        st.metric("NEUTRAL Days", n_neutral)
        st.metric("OVEREXTENDED Days", n_over)

    with col2:
        fig = go.Figure()
        for state in ["LONG_SETUP", "NEUTRAL", "OVEREXTENDED"]:
            fig.add_trace(
                go.Bar(
                    x=[state],
                    y=[dist[state]],
                    name=state,
                    marker=dict(color=S0_SIGNAL_COLORS.get(state, "#999999")),
                    hovertemplate=f"{state}: %{{y}} days<extra></extra>",
                )
            )
        fig.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            yaxis=dict(title="Days"),
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------
# Recent history table (inspectability)
# ---------------------------------------------------------------------
st.subheader("🔎 Recent History (Inspectable)")
st.caption("Last 90 rows for quick inspection and debugging.")

recent = hist.sort_values("trade_date", ascending=False).head(90)

def highlight_state(val: str) -> str:
    color = S0_SIGNAL_COLORS.get(val, "#FFFFFF")
    return f"background-color: {color}; color: white;"

st.dataframe(
    recent[
        [
            "trade_date",
            "ticker",
            "regime_bucket_10",
            "zscore_bucket_10",
            "price_pos_200d",
            "price_zscore_20d",
            "core_signal_state",
            "core_score",
        ]
    ]
    .style.applymap(highlight_state, subset=["core_signal_state"]),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "ⓘ This page is sourced from `signal_core` only. "
    "Performance and forward returns are intentionally excluded."
)
