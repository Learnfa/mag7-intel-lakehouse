import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from utils.data_loaders import (
    load_market_sentiment_latest,
    load_market_sentiment_history,
)
from components.metrics import kpi_row
from components.freshness import data_freshness_panel


st.set_page_config(
    page_title="Market & Sentiment Analysis | MAG7 Intel",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Market & Sentiment Analysis")
st.caption("Price trend vs sentiment regime • Contextual analysis")

# ---------------------------------------------------------------------
# Load latest snapshot (for selector defaults + sidebar freshness)
# ---------------------------------------------------------------------
with st.spinner("Loading latest market & sentiment snapshot…"):
    latest_df = load_market_sentiment_latest()

if latest_df.empty:
    st.error("No data found in `mart.market_sentiment_ts`.")
    st.stop()

latest_df = latest_df.copy()
latest_df["trade_date"] = pd.to_datetime(latest_df["trade_date"])
asof_date = latest_df["trade_date"].max()
tickers = sorted(latest_df["ticker"].dropna().unique().tolist())

data_freshness_panel(
    asof_date=asof_date,
    sources=["mag7_intel_mart.market_sentiment_ts"],
    location="sidebar",
)

# ---------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")

    selected_ticker = st.selectbox("Ticker", options=tickers, index=0)

    bench_choice = st.radio("Benchmark", options=["^NDX", "^NDXE"], horizontal=True)
    freq = st.radio("Aggregation", options=["Daily", "Weekly"], horizontal=True)

    base_index = st.number_input("Normalise base", min_value=10, max_value=500, value=100, step=10)

    st.divider()
    st.markdown("### Display")
    show_macro_shading = st.checkbox("Show macro regime shading", value=True)
    show_gdelt_line = st.checkbox("Show GDELT tone line", value=False)
    show_recent_table = st.checkbox("Show recent rows", value=True)

    st.divider()
    st.markdown("### Date range")
    start_date = st.date_input("Start", value=None)
    end_date = st.date_input("End", value=None)

# ---------------------------------------------------------------------
# Load history for selected ticker
# ---------------------------------------------------------------------
with st.spinner(f"Loading market & sentiment history for {selected_ticker}…"):
    hist = load_market_sentiment_history(
        ticker=selected_ticker,
        start_date=str(start_date) if start_date else None,
        end_date=str(end_date) if end_date else None,
    )

if hist.empty:
    st.warning(f"No history found for ticker: {selected_ticker}")
    st.stop()

hist = hist.copy()
hist["trade_date"] = pd.to_datetime(hist["trade_date"])
hist = hist.sort_values("trade_date")

# ---------------------------------------------------------------------
# Optional weekly aggregation
# ---------------------------------------------------------------------
def _to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    d = df.set_index("trade_date").sort_index()

    out = pd.DataFrame(index=d.resample("W-FRI").last().index)
    out["ticker"] = d["ticker"].resample("W-FRI").last()

    # Price: last
    out["adj_close"] = d["adj_close"].resample("W-FRI").last()
    out["ndx_adj_close"] = d["ndx_adj_close"].resample("W-FRI").last()
    out["ndxe_adj_close"] = d["ndxe_adj_close"].resample("W-FRI").last()

    # Sentiment: counts sum, rates/means average
    out["article_count"] = d["article_count"].resample("W-FRI").sum(min_count=1)
    out["pos_count"] = d["pos_count"].resample("W-FRI").sum(min_count=1)
    out["neg_count"] = d["neg_count"].resample("W-FRI").sum(min_count=1)
    out["neu_count"] = d["neu_count"].resample("W-FRI").sum(min_count=1)

    out["finbert_net_rate"] = d["finbert_net_rate"].resample("W-FRI").mean()
    out["finbert_net_ma7"] = d["finbert_net_ma7"].resample("W-FRI").mean()
    out["sentiment_mean"] = d["sentiment_mean"].resample("W-FRI").mean()

    out["tone_mean"] = d["tone_mean"].resample("W-FRI").mean()
    out["gdelt_tone_ma7"] = d["gdelt_tone_ma7"].resample("W-FRI").mean()

    # Macro: last (already aligned/filled in mart)
    out["macro_regime_4"] = d["macro_regime_4"].resample("W-FRI").last()
    out["macro_risk_off_score_20d"] = d["macro_risk_off_score_20d"].resample("W-FRI").last()
    out["fear_greed"] = d["fear_greed"].resample("W-FRI").last()

    out = out.reset_index().rename(columns={"index": "trade_date"})
    return out


plot_df = _to_weekly(hist) if freq == "Weekly" else hist

# ---------------------------------------------------------------------
# KPI Row
# ---------------------------------------------------------------------
latest_row = plot_df.dropna(subset=["trade_date"]).iloc[-1]

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

bench_val = latest_row["ndx_adj_close"] if bench_choice == "^NDX" else latest_row["ndxe_adj_close"]

kpi_row(
    [
        ("As-of Date", asof_date.strftime("%Y-%m-%d")),
        ("Ticker", selected_ticker),
        ("Macro Regime", str(latest_row.get("macro_regime_4") or "—")),
        ("Fear & Greed", f"{_safe_float(latest_row.get('fear_greed')):.0f}" if pd.notna(latest_row.get("fear_greed")) else "—"),
        ("Risk-off (20d)", f"{_safe_float(latest_row.get('macro_risk_off_score_20d')):.2f}" if pd.notna(latest_row.get("macro_risk_off_score_20d")) else "—"),
        ("Latest Price", f"{_safe_float(latest_row.get('adj_close')):.2f}" if pd.notna(latest_row.get("adj_close")) else "—"),
    ]
)

st.divider()

with st.expander("What does this page show?", expanded=False):
    st.markdown(
        """
- This page **snaps price trend and sentiment trend together**, with **macro regime shading** for context.
- It is **contextual analysis**: *not* a trading signal page and *not* forward-return research.
- Use it to spot **confirmations**, **divergences**, and **macro override** periods.
        """.strip()
    )

# ---------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------
def _normalise(series: pd.Series, base: int = 100) -> pd.Series:
    s = series.astype(float)
    s0 = s.dropna().iloc[0] if len(s.dropna()) else None
    if s0 is None or s0 == 0:
        return pd.Series([None] * len(s), index=s.index)
    return (s / s0) * float(base)

def _add_macro_shading(fig: go.Figure, d: pd.DataFrame) -> None:
    if not show_macro_shading:
        return
    if "macro_regime_4" not in d.columns:
        return

    reg = d[["trade_date", "macro_regime_4"]].dropna().copy()
    if reg.empty:
        return

    reg["chg"] = reg["macro_regime_4"].ne(reg["macro_regime_4"].shift(1))
    chg = reg.loc[reg["chg"], ["trade_date", "macro_regime_4"]].reset_index(drop=True)
    if chg.empty:
        return

    boundaries = list(chg["trade_date"]) + [reg["trade_date"].max()]
    labels = list(chg["macro_regime_4"])

    for i, label in enumerate(labels):
        fig.add_vrect(
            x0=boundaries[i],
            x1=boundaries[i + 1],
            opacity=0.07,
            line_width=0,
            annotation_text=str(label),
            annotation_position="top left",
        )

# ---------------------------------------------------------------------
# Section A — Price trend (Price vs Benchmark, normalised)
# ---------------------------------------------------------------------
bench_col = "ndx_adj_close" if bench_choice == "^NDX" else "ndxe_adj_close"

a = plot_df.copy()
a["price_idx"] = _normalise(a["adj_close"], base_index)
a["bench_idx"] = _normalise(a[bench_col], base_index)

fig_a = go.Figure()
fig_a.add_trace(go.Scatter(x=a["trade_date"], y=a["price_idx"], mode="lines", name=f"{selected_ticker} (indexed)"))
fig_a.add_trace(go.Scatter(x=a["trade_date"], y=a["bench_idx"], mode="lines", name=f"{bench_choice} (indexed)"))
_add_macro_shading(fig_a, a)

fig_a.update_layout(
    title="📈 Price vs Benchmark (Normalised)",
    height=420,
    margin=dict(l=10, r=10, t=60, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    xaxis=dict(title="", showgrid=True),
    yaxis=dict(title=f"Indexed Level (Base={base_index})"),
)

st.plotly_chart(fig_a, use_container_width=True)

# ---------------------------------------------------------------------
# Section B — Sentiment trend (Aggregated)
# ---------------------------------------------------------------------
b = plot_df.copy()
b["pos_bar"] = b["pos_count"]
b["neg_bar"] = -1 * b["neg_count"]

fig_b = go.Figure()
fig_b.add_trace(go.Bar(x=b["trade_date"], y=b["pos_bar"], name="Positive (count)", opacity=0.85))
fig_b.add_trace(go.Bar(x=b["trade_date"], y=b["neg_bar"], name="Negative (count)", opacity=0.85))
fig_b.add_trace(
    go.Scatter(
        x=b["trade_date"],
        y=b["finbert_net_ma7"],
        mode="lines",
        name="FinBERT net (MA7)",
        yaxis="y2",
    )
)

if show_gdelt_line:
    fig_b.add_trace(
        go.Scatter(
            x=b["trade_date"],
            y=b["gdelt_tone_ma7"],
            mode="lines",
            name="GDELT tone (MA7)",
            yaxis="y2",
            line=dict(dash="dot"),
        )
    )

_add_macro_shading(fig_b, b)

fig_b.update_layout(
    title=f"📊 Aggregated Sentiment ({freq})",
    barmode="relative",
    height=420,
    margin=dict(l=10, r=10, t=60, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    xaxis=dict(title="", showgrid=True),
    yaxis=dict(title="Article count (pos/neg)"),
    yaxis2=dict(title="Net sentiment (MA7)", overlaying="y", side="right"),
)

st.plotly_chart(fig_b, use_container_width=True)

# ---------------------------------------------------------------------
# Section C — Snap together (Price vs Sentiment overlay)
# ---------------------------------------------------------------------
c = plot_df.copy()
c["price_idx"] = _normalise(c["adj_close"], base_index)

fig_c = go.Figure()
fig_c.add_trace(go.Scatter(x=c["trade_date"], y=c["price_idx"], mode="lines", name=f"{selected_ticker} (indexed)"))
fig_c.add_trace(go.Scatter(x=c["trade_date"], y=c["finbert_net_ma7"], mode="lines", name="FinBERT net (MA7)", yaxis="y2"))
fig_c.add_trace(
    go.Scatter(
        x=c["trade_date"],
        y=c["macro_risk_off_score_20d"],
        mode="lines",
        name="Macro risk-off (20d)",
        yaxis="y2",
        line=dict(dash="dot"),
    )
)
_add_macro_shading(fig_c, c)

fig_c.update_layout(
    title="📉📊 Price vs Sentiment Overlay",
    height=440,
    margin=dict(l=10, r=10, t=60, b=10),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    xaxis=dict(title="", showgrid=True),
    yaxis=dict(title=f"{selected_ticker} (Indexed Base={base_index})"),
    yaxis2=dict(title="Sentiment / Macro (scaled)", overlaying="y", side="right"),
)

st.plotly_chart(fig_c, use_container_width=True)

# ---------------------------------------------------------------------
# Section D — Interpretation panel (rule-based, no LLM)
# ---------------------------------------------------------------------
def _lead_lag_hint(d: pd.DataFrame):
    x = d[["trade_date", "adj_close", "finbert_net_ma7"]].dropna().copy()
    if x.shape[0] < 40:
        return None, None
    x["ret_1d"] = x["adj_close"].pct_change()
    x = x.dropna()
    if x.shape[0] < 30:
        return None, None

    best_k, best_corr, best_abs = None, None, 0.0
    for k in range(-5, 6):
        s = x["finbert_net_ma7"].shift(k)
        corr = s.corr(x["ret_1d"])
        if corr is None or pd.isna(corr):
            continue
        if abs(corr) > best_abs:
            best_abs, best_k, best_corr = abs(corr), k, float(corr)
    return best_k, best_corr

def _divergence_hint(d: pd.DataFrame) -> str | None:
    x = d[["adj_close", "finbert_net_ma7"]].dropna().tail(25)
    if x.shape[0] < 15:
        return None
    p0, p1 = x["adj_close"].iloc[0], x["adj_close"].iloc[-1]
    s0, s1 = x["finbert_net_ma7"].iloc[0], x["finbert_net_ma7"].iloc[-1]

    price_chg = (p1 - p0) / max(1e-9, p0)
    sent_chg = (s1 - s0)

    if price_chg > 0.03 and sent_chg < -0.02:
        return "Price has trended **up** while sentiment drifted **down** recently → potential **fragile rally / divergence**."
    if price_chg < -0.03 and sent_chg > 0.02:
        return "Price has trended **down** while sentiment improved → possible **stabilisation / early turn**."
    return None

best_k, best_corr = _lead_lag_hint(plot_df)
div_msg = _divergence_hint(plot_df)

latest = plot_df.iloc[-1]
parts = []

regime = latest.get("macro_regime_4", None)
risk = latest.get("macro_risk_off_score_20d", None)
fng = latest.get("fear_greed", None)

macro_line = "Macro context: "
macro_line += f"**{regime}**" if pd.notna(regime) else "—"
if pd.notna(risk):
    macro_line += f" • risk-off (20d) ≈ **{float(risk):.2f}**"
if pd.notna(fng):
    macro_line += f" • Fear & Greed ≈ **{float(fng):.0f}**"
parts.append(macro_line)

if best_k is not None and best_corr is not None:
    if best_k < 0:
        parts.append(f"Lead/lag check: sentiment tends to **lead** ~**{abs(best_k)}** days (corr ≈ **{best_corr:.2f}**).")
    elif best_k > 0:
        parts.append(f"Lead/lag check: sentiment tends to **lag** ~**{best_k}** days (corr ≈ **{best_corr:.2f}**).")
    else:
        parts.append(f"Lead/lag check: strongest relationship is **same-day** (corr ≈ **{best_corr:.2f}**).")
else:
    parts.append("Lead/lag check: insufficient data to estimate a stable relationship.")

parts.append(div_msg or "No strong price–sentiment divergence detected in the recent window (simple slope test).")

recent = plot_df.tail(10).copy()
low_news_days = int((recent["article_count"].fillna(0) < 5).sum())
if low_news_days >= 6:
    parts.append("Data confidence: sentiment is based on **low article volume** on many recent periods — treat small swings as noisy.")
else:
    parts.append("Data confidence: sentiment volume looks **reasonable** for interpreting trend shifts.")

st.subheader("Interpretation")
st.write("\n".join([f"- {p}" for p in parts]))

st.divider()

# ---------------------------------------------------------------------
# Recent history table (inspectability)
# ---------------------------------------------------------------------
if show_recent_table:
    st.subheader("🔎 Recent History (Inspectable)")
    st.caption("Last 90 rows for quick inspection and debugging.")

    cols = [
        "trade_date",
        "ticker",
        "adj_close",
        "ndx_adj_close",
        "ndxe_adj_close",
        "article_count",
        "pos_count",
        "neg_count",
        "finbert_net_ma7",
        "gdelt_tone_ma7",
        "macro_regime_4",
        "macro_risk_off_score_20d",
        "fear_greed",
    ]

    recent_tbl = plot_df.sort_values("trade_date", ascending=False).head(90)
    st.dataframe(
        recent_tbl[cols],
        use_container_width=True,
        hide_index=True,
    )

st.caption("ⓘ This page is sourced from `mart.market_sentiment_ts` (no forward returns).")
