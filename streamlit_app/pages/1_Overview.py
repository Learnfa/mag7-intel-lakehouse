import streamlit as st
import pandas as pd

from utils.data_loaders import (
    load_s0_core_by_date,
    load_s0_core_dates,
    load_price_by_date,
)
from components.metrics import kpi_row
from components.tables import styled_signal_table
from components.banners import production_truth_banner
from components.freshness import data_freshness_panel
from components.date_glider import date_glider
from utils.constants import S0_SIGNAL_COLORS

# ---------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="Overview | MAG7 Signal Intelligence",
    page_icon="🏠",
    layout="wide",
)

# ---------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------
st.title("🏠 Market Overview")
st.caption(
    "Validated core signal snapshot"
)

production_truth_banner()


# ---------------------------------------------------------------------
# Controls (sidebar): date glider + filters
# ---------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Controls")

    with st.spinner("Loading available dates…"):
        dates_list = load_s0_core_dates()

    # HARD sanitize (important)
    if isinstance(dates_list, pd.DataFrame):
        dates_list = dates_list["trade_date"].tolist()
    elif isinstance(dates_list, pd.Series):
        dates_list = dates_list.tolist()
    elif not isinstance(dates_list, list):
        dates_list = list(dates_list)
    
    if not dates_list:
        st.error("No trade dates available.")
        st.stop()

    st.sidebar.caption(f"{len(dates_list)} trading dates available")

    asof_date = date_glider(
        dates_list,
        label="As-at Date",
        key="overview_date_glider",
        formatter=lambda d: d.strftime("%Y-%m-%d"),
    )
    
# ---------------------------------------------------------------------
# Load snapshot for selected data
# ---------------------------------------------------------------------
with st.spinner(f"Loading signal snapshot for {asof_date}…"):
    signal_df = load_s0_core_by_date(asof_date)

if signal_df.empty:
    st.error(f"No signal data found for {asof_date}.")
    st.stop()

with st.spinner(f"Loading prices for {asof_date}…"):
    price_df = load_price_by_date(asof_date)

# Ensure date types
signal_df["trade_date"] = pd.to_datetime(signal_df["trade_date"]).dt.date
if not price_df.empty and "trade_date" in price_df.columns:
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"]).dt.date
    

# ---------------------------------------------------------------------
# Join signal + price (UI only)
# ---------------------------------------------------------------------
df = signal_df.copy()

if not price_df.empty:
    # preferred join: ticker + trade_date
    if {"ticker", "trade_date"}.issubset(price_df.columns):
        df = df.merge(
            price_df[["ticker", "trade_date", "adj_close"]],
            on=["ticker", "trade_date"],
            how="left",
            validate="one_to_one",
        )
    else:
        df = df.merge(
            price_df,
            on=["ticker"],
            how="left",
            validate="one_to_one",
        )
        

# ---------------------------------------------------------------------
# Sidebar: Freshness panel + filters
# ---------------------------------------------------------------------
data_freshness_panel(
    asof_date=asof_date,
    sources=["_mart.signal_core", "_core.fact_prices"],
    location="sidebar",
)

with st.sidebar:

    st.markdown(f"### Signal States")
    selected_states = st.multiselect(
        "",
        options=["LONG_SETUP", "NEUTRAL", "OVEREXTENDED", "MISSING"],
        default=["LONG_SETUP", "NEUTRAL", "OVEREXTENDED"],
        key="overview_states",
        label_visibility="collapsed",
    )

    st.markdown(f"### Tickers")
    selected_tickers = st.multiselect(
        "",
        options=sorted(df["ticker"].unique()),
        default=sorted(df["ticker"].unique()),
        key="overview_tickers",
        label_visibility="collapsed",
    )

filtered_df = df[
    df["core_signal_state"].isin(selected_states)
    & df["ticker"].isin(selected_tickers)
].copy()

# ---------------------------------------------------------------------
# KPI ROW
# ---------------------------------------------------------------------
n_total = df["ticker"].nunique()
n_long = (df["core_signal_state"] == "LONG_SETUP").sum()
n_over = (df["core_signal_state"] == "OVEREXTENDED").sum()
n_neutral = (df["core_signal_state"] == "NEUTRAL").sum()

kpi_row(
    [
        ("As-of Date", str(asof_date)),
        ("Tickers Covered", n_total),
        ("LONG_SETUP", n_long),
        ("OVEREXTENDED", n_over),
        ("NEUTRAL", n_neutral),
    ]
)

st.divider()

# ---------------------------------------------------------------------
# Main Signal Radar Table
# ---------------------------------------------------------------------
st.subheader("📡 Signal Radar")
st.caption(
    "Daily canonical signal state per ticker. "
    "This table reflects **validated logic only**."
)

table_cols = [
    "ticker",
    "adj_close",
    "regime_bucket_10",
    "zscore_bucket_10",
    "regime_label",
    "core_signal_state",
    "core_reason",
    "core_score",
]
table_cols = [c for c in table_cols if c in filtered_df.columns]

styled_signal_table(
    filtered_df[table_cols].sort_values(["core_signal_state", "ticker"]),
    signal_col="core_signal_state",
    color_map=S0_SIGNAL_COLORS,
)

# ---------------------------------------------------------------------
# Explainability footnote (requested)
# ---------------------------------------------------------------------
with st.expander("📘 How signal states & scores are deduced"):
    st.markdown("""
**Buckets**
- `regime_bucket_10`: price percentile vs rolling history (1 = cheapest, 10 = most expensive)
- `zscore_bucket_10`: standardized deviation bucket (1 = cheap, 10 = extended)

**Signal states**
- **LONG_SETUP**: regime ≤ 3 AND z-score ≤ 3  
- **OVEREXTENDED**: regime ≥ 8 AND z-score ≥ 8  
- **NEUTRAL**: everything else  
- **MISSING**: any bucket is null  

**Core score**
- Measures *cheapness only* (0..6).  
- `0` = not attractive / neutral / expensive  
- Higher = stronger long setup.

**Reason**
- `core_reason` explains why a ticker is not a LONG_SETUP (or why missing).
""")

# ---------------------------------------------------------------------
# Footer notes
# ---------------------------------------------------------------------
st.divider()