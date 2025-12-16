import streamlit as st
from google.cloud import bigquery
from .bq_client import run_query

from config.settings import (
    TABLE_S0_CORE_VALUE,
    TABLE_S1_CORE_MOMREV,
    TABLE_FACT_PRICES,
    TABLE_MART_REGIME_SUMMARY,
    TABLE_MART_RISK,
    TABLE_MART_MACRO_RISK_TS,
    TABLE_MART_MARKET_SENTIMENT_TS,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

LATEST_DATE_FILTER = """
QUALIFY trade_date = MAX(trade_date) OVER ()
"""

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

def _param_config(params: dict):
    """
    Build BigQuery parameterized query config.
    """
    return bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(k, "STRING", v)
            for k, v in params.items()
        ]
    )

# ---------------------------------------------------------------------
# Market Sentiment Loaders
# ---------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_market_sentiment_latest():
    """
    Latest snapshot for market & sentiment page (ticker selector + as-of).
    """
    sql = f"""
    SELECT trade_date, ticker
    FROM `{TABLE_MART_MARKET_SENTIMENT_TS}`
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_market_sentiment_latest():
    """
    Latest snapshot for market & sentiment page (ticker selector + as-of).
    """
    sql = f"""
    SELECT trade_date, ticker
    FROM `{TABLE_MART_MARKET_SENTIMENT_TS}`
    """
    return run_query(sql)


# ---------------------------------------------------------------------
# Core S0 Signal Loaders
# ---------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_s0_core_latest():
    """
    Latest snapshot of canonical core signal (one row per ticker).
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_S0_CORE_VALUE}`
    {LATEST_DATE_FILTER}
    ORDER BY ticker
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_s0_core_history(ticker: str):
    """
    Full signal history for a single ticker.
    Used by Core Signal & Deep Dive pages.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_S0_CORE_VALUE}`
    WHERE ticker = @ticker
    ORDER BY trade_date
    """
    return run_query(
        sql,
        job_config=_param_config({"ticker": ticker}),
    )

@st.cache_data(ttl=300)
def load_s0_core_by_date(trade_date):
    """
    Signal snapshot for ALL tickers on a single trade_date.
    Used by Overview / Radar pages.
    """
    trade_date_str = (
        trade_date.strftime("%Y-%m-%d")
        if hasattr(trade_date, "strftime")
        else str(trade_date)
    )
    sql = f"""
    SELECT *
    FROM `{TABLE_S0_CORE_VALUE}`
    WHERE trade_date = @trade_date
    ORDER BY ticker
    """
    return run_query(
        sql,
        job_config=_param_config({"trade_date": trade_date_str}),
    )

@st.cache_data(ttl=300)
def load_s0_core_asof(trade_date: str):
    """
    Signal snapshot as-of a specific date.
    Useful for historical inspection.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_S0_CORE_VALUE}`
    WHERE trade_date = @trade_date
    ORDER BY ticker
    """
    return run_query(
        sql,
        job_config=_param_config({"trade_date": trade_date}),
    )

@st.cache_data(ttl=300)
def load_s0_core_dates():
    """
    All available trading dates in signal_core.
    Used to drive date gliders / selectors.
    """
    sql = f"""
    SELECT DISTINCT trade_date
    FROM `{TABLE_S0_CORE_VALUE}`
    ORDER BY trade_date
    """
    return run_query(sql)["trade_date"].tolist()


# ---------------------------------------------------------------------
# S1: Momentum / Reversion core signal loaders
# ---------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_s1_core_latest():
    """
    Latest snapshot of S1 MOM / REV / NEU signal
    (one row per ticker).
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_S1_CORE_MOMREV}`
    {LATEST_DATE_FILTER}
    ORDER BY ticker
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_s1_core_history(ticker: str):
    """
    Full S1 signal history for a single ticker.
    Used by S1 shading & deep dive pages.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_S1_CORE_MOMREV}`
    WHERE ticker = @ticker
    ORDER BY trade_date
    """
    return run_query(
        sql,
        job_config=_param_config({"ticker": ticker}),
    )


# ---------------------------------------------------------------------
# Price Overview Loaders
# ---------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_price_overview_latest():
    """
    Latest adj_close price per ticker for UI joins.
    """
    sql = f"""
    SELECT ticker, adj_close
    FROM `{TABLE_FACT_PRICES}`
    {LATEST_DATE_FILTER}
    ORDER BY ticker
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_price_by_date(trade_date):
    """
    Daily adjusted close per ticker for ONE trade_date.
    Used by Overview UI only.
    """
    trade_date_str = (
        trade_date.strftime("%Y-%m-%d")
        if hasattr(trade_date, "strftime")
        else str(trade_date)
    )
    sql = f"""
    SELECT
      ticker,
      trade_date,
      adj_close
    FROM `{TABLE_FACT_PRICES}`
    WHERE trade_date = @trade_date
    """
    return run_query(
        sql,
        job_config=_param_config({"trade_date": trade_date_str}),
    )

# ---------------------------------------------------------------------
# Price Corridor Loaders
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_price_corridor_history(ticker: str):
    """
    Load adj_close price with rolling 200-day min/max corridor.

    Returns:
      trade_date, adj_close, roll_min_200d, roll_max_200d
    """
    sql = f"""
    SELECT
      trade_date,
      ticker,
      adj_close,
      MIN(adj_close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
      ) AS roll_min_200d,
      MAX(adj_close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
      ) AS roll_max_200d
    FROM `{TABLE_FACT_PRICES}`
    WHERE ticker = @ticker
    ORDER BY trade_date
    """
    return run_query(sql, job_config=_param_config({"ticker": ticker}))

# ---------------------------------------------------------------------
# Regime Loaders
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_regime_summary():
    """
    Regime summary mart for distribution + diagnostics.
    Expected columns (typical): ticker, regime_bucket_10, n_obs, pct_obs, avg_fwd_ret_20d, etc.
    """
    sql = f"SELECT * FROM `{TABLE_MART_REGIME_SUMMARY}`"
    return run_query(sql)

@st.cache_data(ttl=300)
def load_risk_dashboard_latest():
    """
    Latest risk snapshot per ticker.
    Expected columns depend on your mart, but must include: trade_date, ticker.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_MART_RISK}`
    ORDER BY ticker
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_macro_risk_latest():
    """
    Latest macro risk snapshot.
    Expected columns: trade_date + some macro metrics.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_MART_MACRO_RISK_TS}`
    QUALIFY trade_date = MAX(trade_date) OVER ()
    ORDER BY trade_date
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_macro_risk_history():
    """
    Macro risk history.
    """
    sql = f"""
    SELECT *
    FROM `{TABLE_MART_MACRO_RISK_TS}`
    ORDER BY trade_date
    """
    return run_query(sql)
