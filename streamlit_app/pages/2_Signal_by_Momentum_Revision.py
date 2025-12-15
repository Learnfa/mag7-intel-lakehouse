import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.content_loaders import load_markdown
from utils.data_loaders import load_s1_core_latest, load_s1_core_history
from components.banners import production_truth_banner
from components.metrics import kpi_row
from components.freshness import data_freshness_panel
from utils.constants import S1_SIGNAL_COLORS

st.set_page_config(
    page_title="S1 MOM/REV Signal | Shading + Evidence",
    page_icon="📈",
    layout="wide",
)

st.title("📈 S1 Signal: Momentum vs Mean Reversion (Bucket × Trend × Volatility)")
st.caption("Signal monitoring + forward-return evidence • Compare (All days) vs (Entry days)")

production_truth_banner()

# ---------------------------------------------------------------------
# Load latest snapshot (selector defaults + freshness)
# ---------------------------------------------------------------------
with st.spinner("Loading latest S1 signal snapshot…"):
    latest_df = load_s1_core_latest()

if latest_df.empty:
    st.error("No data found in `mart.s1_core_momrev`.")
    st.stop()

asof_date = pd.to_datetime(latest_df["trade_date"]).max()
tickers = sorted(latest_df["ticker"].unique())

data_freshness_panel(
    asof_date=asof_date,
    sources=["mag7_intel_mart.s1_core_momrev"],
    location="sidebar",
)

# Sidebar controls
with st.sidebar:
    st.markdown("## Controls")
    selected_ticker = st.selectbox("Ticker", options=tickers, index=0)

    lookback_days = st.selectbox("Lookback window", [180, 365, 730, 1460], index=1)

    st.markdown("### Charts")
    show_shading = st.checkbox("Show Chart A (Shading)", value=True)
    show_entries = st.checkbox("Show Chart B (Entries)", value=True)
    show_distributions = st.checkbox("Show Chart C (Distributions)", value=True)
    show_summary = st.checkbox("Show Chart D (Summary)", value=True)
    show_recent_table = st.checkbox("Show recent history table", value=True)

# ---------------------------------------------------------------------
# Load history for selected ticker
# ---------------------------------------------------------------------
with st.spinner(f"Loading S1 history for {selected_ticker}…"):
    hist = load_s1_core_history(selected_ticker)

if hist.empty:
    st.warning(f"No history found for ticker: {selected_ticker}")
    st.stop()

hist = hist.copy()
hist["trade_date"] = pd.to_datetime(hist["trade_date"])
hist = hist.sort_values("trade_date")

# Trim lookback
cutoff = hist["trade_date"].max() - pd.Timedelta(days=int(lookback_days))
hist = hist[hist["trade_date"] >= cutoff].copy()

# Required columns
required = {
    "trade_date", "ticker", "signal_state", "signal_reason",
    "adj_close", "ma_100", "vola_z20d", "vola_not_top_20_252d",
    "fwd_return_5d", "fwd_return_10d", "fwd_return_20d",
    "regime_bucket_10", "price_zscore_20d",
}
missing = required - set(hist.columns)
if missing:
    st.error(f"Missing required columns in S1 mart for this page: {sorted(missing)}")
    st.stop()

hist["signal_state"] = hist["signal_state"].fillna("NEU")

# ---------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------
STATE_TO_HEX = {k: v["hex"] for k, v in S1_SIGNAL_COLORS.items()}
STATE_TO_RGBA = {k: v["rgba_bg"] for k, v in S1_SIGNAL_COLORS.items()}

# ---------------------------------------------------------------------
# Entry detection (block starts)
# ---------------------------------------------------------------------
hist["prev_state"] = hist["signal_state"].shift(1)
hist["is_entry"] = (hist["signal_state"] != hist["prev_state"]) & hist["signal_state"].isin(["MOM", "REV"])
entries = hist[hist["is_entry"]].copy()

# Evidence bases
all_days_df = hist.copy()
entry_days_df = entries.copy()

# ---------------------------------------------------------------------
# KPI Row (S1-relevant)
# ---------------------------------------------------------------------
latest_row = hist.iloc[-1]
n_days = len(hist)
n_mom = int((hist["signal_state"] == "MOM").sum())
n_rev = int((hist["signal_state"] == "REV").sum())
n_neu = int((hist["signal_state"] == "NEU").sum())

def _safe_rate(series_bool: pd.Series) -> float:
    if series_bool is None or len(series_bool) == 0:
        return float("nan")
    return float(series_bool.mean())

mom_fw10 = hist.loc[hist["signal_state"] == "MOM", "fwd_return_10d"]
neu_fw10 = hist.loc[hist["signal_state"] == "NEU", "fwd_return_10d"]

kpi_row(
    [
        ("As-of Date", asof_date.strftime("%Y-%m-%d")),
        ("Ticker", selected_ticker),
        ("Current State", str(latest_row["signal_state"])),
        ("% MOM / % REV", f"{(n_mom/max(1,n_days))*100:.1f}% / {(n_rev/max(1,n_days))*100:.1f}%"),
        ("MOM win-rate FW10 (all)", f"{_safe_rate(mom_fw10.gt(0)) * 100:.1f}%"
         if len(mom_fw10.dropna()) else "—"),
        ("Avg FW10: MOM vs NEU", f"{mom_fw10.mean():.3%} vs {neu_fw10.mean():.3%}"
         if len(mom_fw10.dropna()) and len(neu_fw10.dropna()) else "—"),
    ]
)

st.divider()

# ---------------------------------------------------------------------
# Explanation (loaded from file)
# ---------------------------------------------------------------------
load_markdown(
    "contents/s1_momrev_explainer.md",
    expanded=False,
    title="What does this page show?"
)
# ---------------------------------------------------------------------
# Legend for shading
# ---------------------------------------------------------------------
st.markdown("**Signal shading legend**")
cols = st.columns(3)
for i, k in enumerate(["MOM", "REV", "NEU"]):
    with cols[i]:
        st.markdown(
            f"- **{S1_SIGNAL_COLORS[k]['label']}** "
            f"<span style='display:inline-block;width:12px;height:12px;"
            f"background:{S1_SIGNAL_COLORS[k]['hex']};"
            f"margin-left:6px;border-radius:2px;'></span>",
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------
def _contiguous_blocks(df: pd.DataFrame):
    if df.empty:
        return
    dates = df["trade_date"].tolist()
    states = df["signal_state"].tolist()
    start = dates[0]
    cur = states[0]
    for i in range(1, len(dates)):
        if states[i] != cur:
            yield start, dates[i], cur
            start = dates[i]
            cur = states[i]
    yield start, dates[-1], cur


def _chart_shading(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()

    for x0, x1, state in _contiguous_blocks(df):
        fig.add_shape(
            type="rect",
            xref="x",
            yref="paper",
            x0=x0,
            x1=x1,
            y0=0,
            y1=1,
            fillcolor=STATE_TO_RGBA.get(state, STATE_TO_RGBA["NEU"]),
            line=dict(width=0),
            layer="below",
        )

    fig.add_trace(go.Scatter(
        x=df["trade_date"], y=df["adj_close"],
        mode="lines", name="adj_close",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>adj_close: %{y:.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["trade_date"], y=df["ma_100"],
        mode="lines", name="ma_100",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>ma_100: %{y:.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["trade_date"], y=df["vola_z20d"],
        mode="lines", name="vola_z20d",
        yaxis="y2",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>vola_z20d: %{y:.2f}<extra></extra>",
    ))

    fig.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(title="", showgrid=True),
        yaxis=dict(title="Price"),
        yaxis2=dict(title="vola_z20d", overlaying="y", side="right", showgrid=False),
    )
    return fig


def _chart_entries_on_price(df: pd.DataFrame, entry_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["trade_date"], y=df["adj_close"],
        mode="lines", name="adj_close",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>adj_close: %{y:.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["trade_date"], y=df["ma_100"],
        mode="lines", name="ma_100",
        hovertemplate="<b>%{x|%Y-%m-%d}</b><br>ma_100: %{y:.2f}<extra></extra>",
    ))

    for state in ["MOM", "REV"]:
        sub = entry_df[entry_df["signal_state"] == state]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["trade_date"],
            y=sub["adj_close"],
            mode="markers",
            name=f"{state} entry",
            marker=dict(size=10, symbol="circle", color=STATE_TO_HEX.get(state, "#999999")),
            customdata=sub[[
                "fwd_return_5d","fwd_return_10d","fwd_return_20d",
                "regime_bucket_10","price_zscore_20d","vola_z20d"
            ]],
            hovertemplate=(
                "<b>%{x|%Y-%m-%d}</b><br>"
                f"Entry: {state}<br>"
                "FW5d: %{customdata[0]:.3%}<br>"
                "FW10d: %{customdata[1]:.3%}<br>"
                "FW20d: %{customdata[2]:.3%}<br>"
                "Regime bucket: %{customdata[3]}<br>"
                "Price Z(20d): %{customdata[4]:.2f}<br>"
                "Vol Z(20d): %{customdata[5]:.2f}<br>"
                "<extra></extra>"
            ),
        ))

    fig.update_layout(
        height=460,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(title="", showgrid=True),
        yaxis=dict(title="Price"),
    )
    return fig


def _evidence_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for state in ["MOM", "REV", "NEU"]:
        sub = df[df["signal_state"] == state]
        fw5 = sub["fwd_return_5d"].dropna()
        fw10 = sub["fwd_return_10d"].dropna()
        fw20 = sub["fwd_return_20d"].dropna()
        rows.append({
            "state": state,
            "n": int(len(sub)),
            "mean_fw5": (fw5.mean() if len(fw5) else None),
            "median_fw5": (fw5.median() if len(fw5) else None),
            "win_rate_fw5": (float((fw5 > 0).mean()) if len(fw5) else None),
            "mean_fw10": (fw10.mean() if len(fw10) else None),
            "median_fw10": (fw10.median() if len(fw10) else None),
            "win_rate_fw10": (float((fw10 > 0).mean()) if len(fw10) else None),
            "mean_fw20": (fw20.mean() if len(fw20) else None),
            "median_fw20": (fw20.median() if len(fw20) else None),
            "win_rate_fw20": (float((fw20 > 0).mean()) if len(fw20) else None),
        })
    return pd.DataFrame(rows)


def _chart_evidence_distributions(df: pd.DataFrame, horizon_col: str, title: str) -> go.Figure:
    fig = go.Figure()
    for state in ["MOM", "REV", "NEU"]:
        sub = df[df["signal_state"] == state]
        y = sub[horizon_col].dropna()
        fig.add_trace(go.Box(
            y=y,
            name=state,
            boxpoints="outliers",
            marker=dict(color=STATE_TO_HEX.get(state, "#999999")),
            hovertemplate=f"{state}<br>{horizon_col}: %{{y:.3%}}<extra></extra>",
        ))
    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=30, b=10),
        title=title,
        yaxis=dict(title="Forward return"),
        xaxis=dict(title=""),
        showlegend=False,
    )
    return fig


# ---------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------
if show_shading:
    st.subheader("A) Momentum / Mean-Reversion Signal by Bucket × Trend × Volatility")
    st.plotly_chart(_chart_shading(hist), use_container_width=True)
    st.caption("Background shading is `signal_state` (MOM/REV/NEU). Lines show price, MA100, and vola_z20d.")

if show_entries:
    st.subheader("B) Entry markers (MOM/REV block starts) with forward-return hover evidence")
    st.plotly_chart(_chart_entries_on_price(hist, entries), use_container_width=True)
    st.caption("Entry day = first day the signal switches into MOM or REV.")

basis = st.radio(
    "Evidence basis",
    ["All days (within state)", "Entry days only (block starts)"],
    index=0,
    horizontal=True,
)
evidence_df = all_days_df if basis.startswith("All days") else entry_days_df

if show_distributions:
    st.subheader("C) Evidence distributions (FW5 / FW10 / FW20) by state")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.plotly_chart(_chart_evidence_distributions(
            evidence_df, "fwd_return_5d", f"FW5 by state — {basis}"
        ), use_container_width=True)
    with c2:
        st.plotly_chart(_chart_evidence_distributions(
            evidence_df, "fwd_return_10d", f"FW10 by state — {basis}"
        ), use_container_width=True)
    with c3:
        st.plotly_chart(_chart_evidence_distributions(
            evidence_df, "fwd_return_20d", f"FW20 by state — {basis}"
        ), use_container_width=True)

if show_summary:
    st.subheader("D) Evidence summary (count / mean / median / win-rate)")
    summary = _evidence_summary(evidence_df)

    def _fmt_pct(x):
        return "—" if pd.isna(x) else f"{x:.3%}"

    def _fmt_rate(x):
        return "—" if pd.isna(x) else f"{x*100:.1f}%"

    show = summary.copy()
    for col in ["mean_fw5","median_fw5","mean_fw10","median_fw10","mean_fw20","median_fw20"]:
        show[col] = show[col].apply(_fmt_pct)
    for col in ["win_rate_fw5","win_rate_fw10","win_rate_fw20"]:
        show[col] = show[col].apply(_fmt_rate)

    st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption("Win-rate = forward return > 0 under the selected basis.")

st.divider()

if show_recent_table:
    st.subheader("🔎 Recent history (inspectable)")
    st.caption("Last 90 rows for quick debugging.")
    recent = hist.sort_values("trade_date", ascending=False).head(90)

    def highlight_state(val: str) -> str:
        color = STATE_TO_HEX.get(val, "#FFFFFF")
        txt = "white" if val in ("MOM", "REV", "MISSING") else "black"
        return f"background-color: {color}; color: {txt};"

    cols = [
        "trade_date","ticker","signal_state","signal_reason",
        "adj_close","ma_100","regime_bucket_10","price_zscore_20d","vola_z20d","vola_not_top_20_252d",
        "fwd_return_5d","fwd_return_10d","fwd_return_20d",
    ]
    cols = [c for c in cols if c in recent.columns]

    st.dataframe(
        recent[cols].style.applymap(highlight_state, subset=["signal_state"]),
        use_container_width=True,
        hide_index=True,
    )

st.caption(
    "ⓘ Source: `mart.s1_core_momrev`. Evidence is forward returns (5/10/20d). "
    "Equity curves and portfolio backtests belong in separate research pages."
)
