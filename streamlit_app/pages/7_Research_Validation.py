import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from google.cloud import bigquery

from utils.bq_client import run_query
from utils.constants import S0_SIGNAL_COLORS

from components.banners import research_warning_banner
from components.freshness import data_freshness_panel

# Optional: use settings constants if you have them
try:
    from config.settings import TABLE_S0_RESEARCH_PERF, TABLE_S0_RESEARCH_EVENTS
except Exception:
    TABLE_S0_RESEARCH_PERF = "mag7_intel_mart.s0_research_performance"
    TABLE_S0_RESEARCH_EVENTS = "mag7_intel_mart.s0_research_events"


st.set_page_config(
    page_title="Research Validation | MAG7 Intel",
    page_icon="🧪",
    layout="wide",
)

st.title("🧪 Research & Validation")
st.caption("NB3/NB4-style evidence • EARLY/LATE robustness • Look-ahead metrics (research only)")

research_warning_banner()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _param_config(params: dict) -> bigquery.QueryJobConfig:
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING", v)
            for k, v in params.items()
        ]
    )

@st.cache_data(ttl=300)
def load_research_performance() -> pd.DataFrame:
    sql = f"""
    SELECT *
    FROM `{TABLE_S0_RESEARCH_PERF}`
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_heatmap_surface(horizon: int, period_label: str, state: str) -> pd.DataFrame:
    """
    Build a regime_bucket_10 × zscore_bucket_10 surface of avg forward return
    from signal_research_events (row-level research table).

    period_label: EARLY/LATE/FULL
    state: LONG_SETUP / NEUTRAL / OVEREXTENDED
    """
    # Map horizon to column name in events table
    col = {5: "fwd_ret_5d", 10: "fwd_ret_10d", 20: "fwd_ret_20d"}.get(horizon, "fwd_ret_20d")

    # FULL means no period filter
    period_where = ""
    if period_label in ("EARLY", "LATE"):
        period_where = "AND period_label = @period_label"

    sql = f"""
    SELECT
      regime_bucket_10,
      zscore_bucket_10,
      COUNT(*) AS n_obs,
      AVG({col}) AS avg_forward_return,
      COUNTIF({col} > 0) / COUNT(*) AS win_rate
    FROM `{TABLE_S0_RESEARCH_EVENTS}`
    WHERE core_signal_state = @state
      {period_where}
      AND {col} IS NOT NULL
    GROUP BY 1,2
    ORDER BY 1,2
    """
    params = {"state": state}
    if period_label in ("EARLY", "LATE"):
        params["period_label"] = period_label

    return run_query(sql, job_config=_param_config(params))


# ---------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------
with st.spinner("Loading research validation tables…"):
    perf = load_research_performance()

if perf.empty:
    st.error("No rows returned from signal_research_performance.")
    st.stop()

# Normalize column names defensively (in case types come in as objects)
perf = perf.copy()

# Expected columns in the aggregate table
required_cols = {
    "ticker", "period_label", "horizon", "core_signal_state",
    "n_obs", "avg_forward_return", "win_rate"
}
missing = required_cols - set(perf.columns)
if missing:
    st.error(
        "signal_research_performance is missing expected columns:\n"
        f"- {', '.join(sorted(missing))}\n\n"
        "Either adjust this page to your table schema, or update the mart to include these fields."
    )
    st.stop()

# Prefer real trade_date if present; otherwise show None (or "—")
asof_date = None
if "trade_date" in perf.columns:
    asof_date = pd.to_datetime(perf["trade_date"], errors="coerce").max()

data_freshness_panel(
    asof_date=asof_date,
    sources=[
        "mag7_intel_mart.signal_research_performance",
        "mag7_intel_mart.signal_research_events",
    ],
    location="sidebar",
)

# ---------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")

    tickers = sorted(perf["ticker"].dropna().unique().tolist())
    states = ["LONG_SETUP", "NEUTRAL", "OVEREXTENDED"]
    horizons = sorted(perf["horizon"].dropna().unique().tolist())
    periods = ["FULL", "EARLY", "LATE"]

    selected_tickers = st.multiselect("Tickers", options=tickers, default=tickers)
    selected_state = st.selectbox("Signal State", options=states, index=0)
    selected_horizon = st.selectbox("Horizon (days)", options=horizons, index=horizons.index(20) if 20 in horizons else 0)
    selected_periods = st.multiselect("Period Label", options=periods, default=["FULL", "EARLY", "LATE"])

# Filtered view
view = perf[
    perf["ticker"].isin(selected_tickers)
    & (perf["core_signal_state"] == selected_state)
    & (perf["horizon"] == selected_horizon)
    & (perf["period_label"].isin(selected_periods))
].copy()

if view.empty:
    st.warning("No rows match your filters.")
    st.stop()

# ---------------------------------------------------------------------
# Section 1: Summary table
# ---------------------------------------------------------------------
st.subheader("📋 Validation Summary")
st.caption("Aggregate forward-return evidence by ticker × period × horizon (research-only).")

# Pretty formatting
view_display = view[
    ["ticker", "period_label", "horizon", "core_signal_state", "n_obs", "avg_forward_return", "win_rate"]
].copy()

view_display["avg_forward_return"] = pd.to_numeric(view_display["avg_forward_return"], errors="coerce")
view_display["win_rate"] = pd.to_numeric(view_display["win_rate"], errors="coerce")

st.dataframe(
    view_display.sort_values(["ticker", "period_label"]),
    use_container_width=True,
    hide_index=True,
)

st.divider()

# ---------------------------------------------------------------------
# Section 2: EARLY vs LATE comparison (pooled across selected tickers)
# ---------------------------------------------------------------------
st.subheader("🧷 Robustness Check: EARLY vs LATE")
st.caption("Pooled comparison across selected tickers for the chosen state & horizon.")

pool = perf[
    perf["ticker"].isin(selected_tickers)
    & (perf["core_signal_state"] == selected_state)
    & (perf["horizon"] == selected_horizon)
    & (perf["period_label"].isin(["EARLY", "LATE"]))
].copy()

if pool.empty:
    st.info("No EARLY/LATE rows available for this selection.")
else:
    # Weighted average by n_obs (more honest than simple mean)
    pool["avg_forward_return"] = pd.to_numeric(pool["avg_forward_return"], errors="coerce")
    pool["win_rate"] = pd.to_numeric(pool["win_rate"], errors="coerce")
    pool["n_obs"] = pd.to_numeric(pool["n_obs"], errors="coerce")

    def wavg(series, weights):
        return (series * weights).sum() / weights.sum() if weights.sum() else None

    summary = (
        pool.groupby("period_label")
        .apply(lambda g: pd.Series({
            "n_obs": int(g["n_obs"].sum()),
            "avg_forward_return_w": float(wavg(g["avg_forward_return"], g["n_obs"])),
            "win_rate_w": float(wavg(g["win_rate"], g["n_obs"])),
        }))
        .reset_index()
    )

    col1, col2 = st.columns(2)

    # Avg forward return bar
    with col1:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=summary["period_label"],
                y=summary["avg_forward_return_w"],
                marker=dict(color=S0_SIGNAL_COLORS.get(selected_state, "#2563EB")),
                hovertemplate="<b>%{x}</b><br>Avg fwd return: %{y:.4f}<extra></extra>",
            )
        )
        fig.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Avg Forward Return"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig, use_container_width=True)

    # Win rate bar
    with col2:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=summary["period_label"],
                y=summary["win_rate_w"],
                marker=dict(color=S0_SIGNAL_COLORS.get(selected_state, "#2563EB")),
                hovertemplate="<b>%{x}</b><br>Win rate: %{y:.2%}<extra></extra>",
            )
        )
        fig.update_layout(
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            yaxis=dict(title="Win Rate", tickformat=".0%"),
            xaxis=dict(title=""),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "ⓘ Bars use **weighted averages** by `n_obs` to avoid small-sample tickers dominating pooled results."
    )

st.divider()

# ---------------------------------------------------------------------
# Section 3: Regime × Z heatmap surface (optional but valuable)
# ---------------------------------------------------------------------
st.subheader("🗺️ Regime × Z-Score Surface (Research Heatmap)")
st.caption(
    "Average forward return by (regime_bucket_10 × zscore_bucket_10). "
    "This is built from `signal_research_events` and contains look-ahead metrics."
)

colA, colB = st.columns([1, 2])
with colA:
    hm_period = st.selectbox("Heatmap Period", options=["FULL", "EARLY", "LATE"], index=0)

with st.spinner("Building heatmap surface…"):
    surface = load_heatmap_surface(
        horizon=int(selected_horizon),
        period_label=hm_period,
        state=selected_state,
    )

if surface.empty:
    st.info("No heatmap data returned for the selected options.")
else:
    # Prepare 10x10 matrix
    surface["regime_bucket_10"] = surface["regime_bucket_10"].astype(int)
    surface["zscore_bucket_10"] = surface["zscore_bucket_10"].astype(int)

    # Matrix initialized with NaNs
    z = [[None for _ in range(10)] for __ in range(10)]
    text = [[None for _ in range(10)] for __ in range(10)]

    for _, row in surface.iterrows():
        r = row["regime_bucket_10"] - 1
        c = row["zscore_bucket_10"] - 1
        z[r][c] = float(row["avg_forward_return"])
        text[r][c] = f"n={int(row['n_obs'])}<br>win={float(row['win_rate']):.0%}"

    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=list(range(1, 11)),
            y=list(range(1, 11)),
            hovertemplate=(
                "Regime bucket: %{y}<br>"
                "Z bucket: %{x}<br>"
                "Avg fwd return: %{z:.4f}<br>"
                "%{text}<extra></extra>"
            ),
            text=text,
            colorbar=dict(title="Avg fwd<br>return"),
        )
    )

    fig.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title="Z-score Bucket (1=oversold → 10=overbought)", dtick=1),
        yaxis=dict(title="Regime Bucket (1=cheap → 10=expensive)", dtick=1),
    )

    st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ The heatmap is **research evidence**. It is not a tradable expected-return surface."
)
