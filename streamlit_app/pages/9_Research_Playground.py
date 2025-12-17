import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from google.cloud import bigquery
from utils.bq_client import run_query

from components.banners import research_danger_banner
from components.freshness import data_freshness_panel

# use settings constants 
try:
    from config.settings import TABLE_SIGNAL_RESEARCH_EVENTS
except Exception:
    TABLE_SIGNAL_RESEARCH_EVENTS = "mag7_intel_mart.signal_research_events"


st.set_page_config(
    page_title="Research Playground | MAG7 Intel",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Research Playground (Advanced)")
st.caption("Exploration only • Contains look-ahead bias • Not a tradable backtest")

research_danger_banner()

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
def load_events(tickers: list[str], period: str) -> pd.DataFrame:
    """
    Load research events for selected tickers, optionally filtered by period_label.
    """
    # FULL means no period filter
    period_where = ""
    params = {}

    if period in ("EARLY", "LATE"):
        period_where = "AND period_label = @period_label"
        params["period_label"] = period

    # Use UNNEST for tickers list
    sql = f"""
    SELECT
      trade_date,
      ticker,
      core_signal_state,
      period_label,
      fwd_ret_5d,
      fwd_ret_10d,
      fwd_ret_20d
    FROM `{TABLE_SIGNAL_RESEARCH_EVENTS}`
    WHERE ticker IN UNNEST(@tickers)
      {period_where}
    ORDER BY trade_date
    """

    query_params = [
        bigquery.ArrayQueryParameter("tickers", "STRING", tickers),
    ]
    if "period_label" in params:
        query_params.append(bigquery.ScalarQueryParameter("period_label", "STRING", params["period_label"]))

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    return run_query(sql, job_config=job_config)

def build_equity_curve(df: pd.DataFrame, ret_col: str, mode: str, horizon: int) -> pd.DataFrame:
    """
    Build a simple cumulative curve from forward returns.

    mode:
      - "all_days": cumulative product of (1 + fwd_ret) each day (demonstration)
      - "long_setup_only": only apply returns on LONG_SETUP days (overlap allowed)
      - "long_setup_no_overlap": only apply returns on LONG_SETUP entry days,
                                 then skip next `horizon` rows (per ticker)
    """
    tmp = df.copy()
    tmp[ret_col] = pd.to_numeric(tmp[ret_col], errors="coerce").fillna(0.0)

    if mode == "all_days":
        tmp["is_trade"] = 1
        tmp["step"] = 1.0 + tmp[ret_col]

    elif mode == "long_setup_only":
        tmp["is_trade"] = (tmp["core_signal_state"] == "LONG_SETUP").astype(int)
        tmp["step"] = 1.0 + (tmp[ret_col] * tmp["is_trade"])

    elif mode == "long_setup_no_overlap":
        is_trade = []
        steps = []
        cooldown = 0

        for state, r in zip(tmp["core_signal_state"].tolist(), tmp[ret_col].tolist()):
            if cooldown > 0:
                # cannot open new trade
                is_trade.append(0)
                steps.append(1.0)
                cooldown -= 1
                continue

            if state == "LONG_SETUP":
                # open trade today, then lock out for next `horizon` rows
                is_trade.append(1)
                steps.append(1.0 + r)
                cooldown = int(horizon)  # skip next horizon rows
            else:
                is_trade.append(0)
                steps.append(1.0)

        tmp["is_trade"] = is_trade
        tmp["step"] = steps

    else:
        raise ValueError(f"Unknown mode: {mode}")

    tmp["equity"] = tmp["step"].cumprod()
    return tmp


# ---------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------
with st.sidebar:
    st.header("## Controls")

    horizon = st.selectbox("Horizon (days)", options=[5, 10, 20], index=2)
    ret_col = {5: "fwd_ret_5d", 10: "fwd_ret_10d", 20: "fwd_ret_20d"}[horizon]

    period = st.selectbox("Period", options=["FULL", "EARLY", "LATE"], index=0)

    curve_mode = st.radio(
        "Curve mode",
        options=[
            ("All days (demonstration)", "all_days"),
            ("LONG_SETUP only (overlap allowed)", "long_setup_only"),
            ("LONG_SETUP only (no-overlap)", "long_setup_no_overlap"),
        ],
        index=2,
        format_func=lambda x: x[0],
    )[1]

    st.divider()
    st.caption("Tickers to plot")

# We need tickers list for selection; query a small distinct list
@st.cache_data(ttl=600)
def list_event_tickers() -> list[str]:
    sql = f"SELECT DISTINCT ticker FROM `{TABLE_SIGNAL_RESEARCH_EVENTS}` ORDER BY ticker"
    df = run_query(sql)
    return df["ticker"].dropna().tolist()

all_tickers = list_event_tickers()

with st.sidebar:
    selected_tickers = st.multiselect(
        "Tickers",
        options=all_tickers,
        default=all_tickers[:7] if len(all_tickers) >= 7 else all_tickers,
    )

if not selected_tickers:
    st.warning("Select at least one ticker.")
    st.stop()

# ---------------------------------------------------------------------
# Load events
# ---------------------------------------------------------------------
with st.spinner("Loading research events…"):
    events = load_events(selected_tickers, period)

if events.empty:
    st.warning("No events returned for this selection.")
    st.stop()

events = events.copy()
events["trade_date"] = pd.to_datetime(events["trade_date"])
events = events.sort_values(["ticker", "trade_date"])

# ✅ Add freshness panel here (before user date filter)
asof_date = events["trade_date"].max()
data_freshness_panel(
    asof_date=asof_date,
    sources=["mag7_intel_mart.signal_research_events"],
    location="sidebar",
)

# Allow date filter locally (no extra query)
min_date = events["trade_date"].min().date()
max_date = events["trade_date"].max().date()

with st.sidebar:
    date_range = st.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
events = events[(events["trade_date"] >= start_date) & (events["trade_date"] <= end_date)]

# ---------------------------------------------------------------------
# Build curves per ticker
# ---------------------------------------------------------------------
st.subheader("📈 Demonstration Equity Curves")
st.caption(
    f"Mode: **{('All days' if curve_mode=='all_days' else 'LONG_SETUP days only')}** • "
    f"Horizon: **{horizon}d** • Period: **{period}**"
)

fig = go.Figure()

for t in selected_tickers:
    df_t = events[events["ticker"] == t].copy()
    if df_t.empty:
        continue

    df_t = build_equity_curve(df_t, ret_col=ret_col, mode=curve_mode, horizon=horizon)
    n_trades = int(df_t["is_trade"].sum())
    
    fig.add_trace(
        go.Scatter(
            x=df_t["trade_date"],
            y=df_t["equity"],
            mode="lines",
            name=t,
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Equity: %{y:.4f}<extra></extra>",
        )
    )

fig.update_layout(
    height=520,
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(title=""),
    yaxis=dict(title="Cumulative (demonstration)", rangemode="tozero"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
)
st.plotly_chart(fig, use_container_width=True)

st.caption(
    "ⓘ Equity curves here are **demonstrations built from look-ahead forward returns**. "
    "They do not account for trade overlap, position sizing, costs, slippage, or execution rules."
)

st.divider()

# ---------------------------------------------------------------------
# Summary stats (demonstration)
# ---------------------------------------------------------------------
st.subheader("📊 Summary (Demonstration)")
st.caption("Simple summary of average forward returns and LONG_SETUP frequency (not tradable stats).")

summary = (
    events.assign(ret=pd.to_numeric(events[ret_col], errors="coerce"))
    .groupby("ticker")
    .agg(
        n_obs=("ret", "count"),
        avg_fwd_return=("ret", "mean"),
        win_rate=("ret", lambda x: float((x > 0).mean()) if len(x) else np.nan),
        long_setup_rate=("core_signal_state", lambda x: float((x == "LONG_SETUP").mean()) if len(x) else np.nan),
    )
    .reset_index()
)

summary["avg_fwd_return"] = summary["avg_fwd_return"].astype(float)
summary["win_rate"] = summary["win_rate"].astype(float)
summary["long_setup_rate"] = summary["long_setup_rate"].astype(float)

st.dataframe(
    summary.sort_values("avg_fwd_return", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "ⓘ These are **conditional forward-return summaries** from `signal_research_events` "
    "and should be used only for research intuition and presentation."
)
