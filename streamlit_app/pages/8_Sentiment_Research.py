import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.bq_client import run_query
from components.banners import research_warning_banner
from components.freshness import data_freshness_panel

TABLE = "mag7_intel_mart.signal_research_sentiment"

st.set_page_config(
    page_title="Sentiment Research | MAG7 Intel",
    page_icon="🧠",
    layout="wide",
)

st.title("🧠 Sentiment vs Forward Returns (Research)")
st.caption("Exploratory sentiment analytics • Research only")

research_warning_banner()

# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_sentiment_research():
    sql = f"SELECT * FROM `{TABLE}`"
    return run_query(sql)

with st.spinner("Loading sentiment research mart…"):
    df = load_sentiment_research()

if df.empty:
    st.warning("No sentiment research data available.")
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"])

# ---------------------------------------------------------------------
# Freshness (after load, before filters)
# ---------------------------------------------------------------------
data_freshness_panel(
    asof_date=df["trade_date"].max(),
    sources=[
        "mag7_intel_mart.signal_research_sentiment",
        "mag7_intel_mart.signal_core",
    ],
    location="sidebar",
)

# ---------------------------------------------------------------------
# Join regime context (research-only join)
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_regime_context_min():
    # Only pull what we actually need for joins + interaction heatmap
    sql = """
    SELECT
      trade_date,
      ticker,
      regime_bucket_10,
      core_signal_state,
      regime_label
    FROM `mag7_intel_mart.signal_core`
    """
    return run_query(sql)

with st.spinner("Loading regime context…"):
    regimes = load_regime_context_min()

if regimes.empty:
    st.warning("No rows returned from `signal_core` for regime context.")
    st.stop()

regimes["trade_date"] = pd.to_datetime(regimes["trade_date"])

df = df.merge(
    regimes,
    on=["trade_date", "ticker"],
    how="inner",
)


# ---------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")

    tickers = sorted(df["ticker"].unique())
    sources = sorted(df["sentiment_source"].unique())

    selected_tickers = st.multiselect("Tickers", options=tickers, default=tickers)
    sentiment_source = st.selectbox("Sentiment Source", options=sources, index=0)
    horizon = st.selectbox("Forward Return Horizon", options=[5, 20], index=1)
    lag = st.slider("Sentiment Lag (days)", -10, 10, 0)

# ---------------------------------------------------------------------
# Filter + lag
# ---------------------------------------------------------------------
view = df[
    df["ticker"].isin(selected_tickers)
    & (df["sentiment_source"] == sentiment_source)
].copy()

view = view.sort_values(["ticker", "trade_date"])
view["sentiment_lagged"] = view.groupby("ticker")["sentiment_score"].shift(lag)

# ---------------------------------------------------------------------
# Sentiment decile buckets (per ticker)
# ---------------------------------------------------------------------
view["sentiment_bucket_10"] = (
    view.groupby("ticker")["sentiment_lagged"]
    .transform(
        lambda x: pd.qcut(x, 10, labels=False, duplicates="drop")
    )
    + 1
)

ret_col = "fwd_return_20d" if horizon == 20 else "fwd_return_5d"
view = view.dropna(subset=["sentiment_lagged", ret_col])

if view.empty:
    st.warning("No data for selected settings.")
    st.stop()

# ---------------------------------------------------------------------
# Correlation table
# ---------------------------------------------------------------------
st.subheader("📋 Correlation Summary")

corr = (
    view.groupby("ticker")
    .apply(lambda g: g["sentiment_lagged"].corr(g[ret_col]) if len(g) > 10 else None)
    .reset_index(name="correlation")
    .sort_values("correlation", ascending=False)
)

st.dataframe(corr, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------
# EARLY vs LATE Comparison
# ---------------------------------------------------------------------
st.subheader("🧪 EARLY vs LATE — Sentiment Robustness")
st.caption(
    "Compare sentiment effectiveness in earlier vs recent history. "
    "Results are pooled across selected tickers."
)

early_late = (
    view.groupby(["regime_label"])
    .apply(
        lambda g: pd.Series({
            "n_obs": len(g),
            "avg_fwd_return": g[ret_col].mean(),
            "correlation": (
                g["sentiment_lagged"].corr(g[ret_col])
                if len(g) > 10 else None
            ),
        })
    )
    .reset_index()
)

if early_late.empty:
    st.info("Not enough data for EARLY/LATE comparison.")
else:
    col1, col2 = st.columns(2)

    # --- Avg forward return ---
    with col1:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=early_late["regime_label"],
                    y=early_late["avg_fwd_return"],
                    hovertemplate="<b>%{x}</b><br>Avg fwd return: %{y:.3%}<extra></extra>",
                )
            ]
        )
        fig.update_layout(
            height=320,
            yaxis=dict(title=f"Avg {horizon}d Forward Return"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- Correlation ---
    with col2:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=early_late["regime_label"],
                    y=early_late["correlation"],
                    hovertemplate="<b>%{x}</b><br>Correlation: %{y:.3f}<extra></extra>",
                )
            ]
        )
        fig.update_layout(
            height=320,
            yaxis=dict(title="Correlation"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "ⓘ EARLY/LATE split tests temporal robustness. "
        "Stronger LATE performance is generally a positive sign."
    )

# ---------------------------------------------------------------------
# Sentiment Bucket Heatmap
# ---------------------------------------------------------------------
st.subheader("🗺️ Sentiment Bucket × Forward Return Heatmap")
st.caption(
    "Average forward return by sentiment decile (1 = most negative, 10 = most positive). "
    "Research-only diagnostic."
)

hm_period = st.selectbox(
    "Heatmap Period",
    options=["ALL", "EARLY", "LATE"],
    index=0,
)

hm_view = view.copy()
if hm_period in ("EARLY", "LATE"):
    hm_view = hm_view[hm_view["regime_label"] == hm_period]

if hm_view.empty:
    st.info("No data available for selected heatmap period.")
else:
    heat = (
        hm_view
        .groupby("sentiment_bucket_10")
        .agg(
            avg_fwd_return=(ret_col, "mean"),
            n_obs=(ret_col, "count"),
        )
        .reset_index()
    )

    fig = go.Figure(
        data=[
            go.Heatmap(
                x=heat["sentiment_bucket_10"],
                y=["Avg Forward Return"],
                z=[heat["avg_fwd_return"]],
                text=[heat["n_obs"]],
                hovertemplate=(
                    "Sentiment bucket: %{x}<br>"
                    "Avg fwd return: %{z:.3%}<br>"
                    "n obs: %{text}<extra></extra>"
                ),
                colorbar=dict(title="Avg fwd return"),
            )
        ]
    )

    fig.update_layout(
        height=260,
        xaxis=dict(title="Sentiment Decile (1 = most negative → 10 = most positive)", dtick=1),
        yaxis=dict(showticklabels=False),
    )

    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ Heatmaps show **conditional averages**, not expected returns. "
    "Bucket boundaries are computed per ticker."
)

# ---------------------------------------------------------------------
# Sentiment × Regime Interaction Heatmap
# ---------------------------------------------------------------------
st.subheader("🧩 Sentiment × Regime Interaction Heatmap")
st.caption(
    "Average forward return conditioned on BOTH sentiment decile and price regime. "
    "Research-only diagnostic."
)

colA, colB = st.columns([1, 2])

with colA:
    inter_period = st.selectbox(
        "Period",
        options=["ALL", "EARLY", "LATE"],
        index=0,
        key="sent_regime_period",
    )

# Filter by period
inter_view = view.copy()
if inter_period in ("EARLY", "LATE"):
    inter_view = inter_view[inter_view["regime_label"] == inter_period]

# Ensure required columns exist
required = {"sentiment_bucket_10", "regime_bucket_10", ret_col}
if not required.issubset(inter_view.columns):
    st.info("Required columns missing to compute interaction heatmap.")
    st.stop()

# Aggregate
interaction = (
    inter_view
    .groupby(["regime_bucket_10", "sentiment_bucket_10"])
    .agg(
        avg_fwd_return=(ret_col, "mean"),
        n_obs=(ret_col, "count"),
    )
    .reset_index()
)

if interaction.empty:
    st.info("No data available for interaction heatmap.")
else:
    # Build 10×10 grid
    z = [[None for _ in range(10)] for __ in range(10)]
    text = [[None for _ in range(10)] for __ in range(10)]

    for _, row in interaction.iterrows():
        r = int(row["regime_bucket_10"]) - 1
        c = int(row["sentiment_bucket_10"]) - 1
        z[r][c] = row["avg_fwd_return"]
        text[r][c] = f"n={int(row['n_obs'])}"

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(1, 11)),
            y=list(range(1, 11)),
            text=text,
            hovertemplate=(
                "Regime bucket: %{y}<br>"
                "Sentiment bucket: %{x}<br>"
                "Avg fwd return: %{z:.3%}<br>"
                "%{text}<extra></extra>"
            ),
            colorbar=dict(title="Avg fwd return"),
        )
    )

    fig.update_layout(
        height=520,
        xaxis=dict(
            title="Sentiment Decile (1 = most negative → 10 = most positive)",
            dtick=1,
        ),
        yaxis=dict(
            title="Regime Bucket (1 = cheap → 10 = expensive)",
            dtick=1,
        ),
        margin=dict(l=10, r=10, t=10, b=10),
    )

    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ Interpretation: look for **diagonal structure** or **corner effects**. "
    "E.g., negative sentiment in cheap regimes vs positive sentiment in expensive regimes."
)

early_late["regime_label"] = pd.Categorical(
    early_late["regime_label"],
    categories=["EARLY", "LATE"],
    ordered=True,
)
early_late = early_late.sort_values("regime_label")

# ---------------------------------------------------------------------
# Scatter plot
# ---------------------------------------------------------------------
st.subheader("📈 Scatter: Sentiment vs Forward Return")

ticker_scatter = st.selectbox(
    "Ticker for scatter plot",
    options=sorted(view["ticker"].unique()),
    index=0,
)

plot_df = view[view["ticker"] == ticker_scatter]

fig = go.Figure(
    data=[
        go.Scatter(
            x=plot_df["sentiment_lagged"],
            y=plot_df[ret_col],
            mode="markers",
            marker=dict(size=6, opacity=0.6),
            hovertemplate=(
                "Date: %{customdata[0]|%Y-%m-%d}<br>"
                "Sentiment: %{x:.3f}<br>"
                "Fwd Return: %{y:.3%}<extra></extra>"
            ),
            customdata=plot_df[["trade_date"]],
        )
    ]
)

fig.update_layout(
    height=420,
    xaxis=dict(title=f"Sentiment (lag {lag}d)"),
    yaxis=dict(title=f"{horizon}d Forward Return"),
)

st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ Correlations and scatter plots are research diagnostics only. "
    "They do not imply tradable sentiment signals."
)
