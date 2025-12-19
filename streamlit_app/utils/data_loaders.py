import streamlit as st
from google.cloud import bigquery
import pandas as pd
from .bq_client import run_query

from config.settings import (
    TABLE_S0_CORE_VALUE,
    TABLE_S1_CORE_MOMREV,
    TABLE_FACT_PRICES,
    TABLE_FACT_PRICE_FEATS,
    TABLE_FACT_MACRO,
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
# Overview Today Loader
# ---------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_overview_today():
    """
    Latest 'today snapshot' for Overview page.

    Source:
      - mag7_intel_mart.overview_today

    Grain:
      - 1 row per ticker (MAG7)

    Purpose:
      - Power the Overview "Today table"
      - Price + TA + Sentiment + Core signal + Macro context
    """
    sql = """
    SELECT *
    FROM `mag7_intel_mart.overview_today`
    ORDER BY ticker
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_overview_signal_snapshot():
    """
    Control-center KPI snapshot for Overview page.

    KPIs:
      - # LONG_SETUP
      - # OVEREXTENDED
      - # MISSING
      - Avg core_score
      - As-of date
    """
    sql = """
    WITH latest AS (
      SELECT MAX(trade_date) AS asof_date
      FROM `mag7_intel_mart.s0_core_value`
    )
    SELECT
      (SELECT asof_date FROM latest) AS asof_date,
      COUNTIF(core_signal_state = 'LONG_SETUP')   AS n_long_setup,
      COUNTIF(core_signal_state = 'OVEREXTENDED') AS n_overextended,
      COUNTIF(core_signal_state = 'MISSING')      AS n_missing,
      AVG(core_score)                             AS avg_core_score
    FROM `mag7_intel_mart.s0_core_value`
    WHERE trade_date = (SELECT asof_date FROM latest)
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_overview_macro_snapshot():
    """
    Latest macro snapshot for Overview page.

    Provides:
      - Fear & Greed index
      - Macro regime label
      - Risk-off score
    """
    sql = """
    WITH latest AS (
      SELECT MAX(trade_date) AS asof_date
      FROM `mag7_intel_mart.macro_risk_ts`
    )
    SELECT
      trade_date,
      fear_greed,
      macro_regime_4,
      macro_risk_off_score_20d
    FROM `mag7_intel_mart.macro_risk_ts`
    WHERE trade_date = (SELECT asof_date FROM latest)
    """
    return run_query(sql)

@st.cache_data(ttl=300)
def load_overview_trending(start_date: str | None = None):
    """
    Time-series data for Overview trending chart.

    Includes:
      - MAG7 prices
      - NDX / NDXE benchmarks
      - Fear & Greed (aligned)

    Params:
      - start_date (YYYY-MM-DD), optional
    """
    where_clause = ""
    if start_date:
        where_clause = f"WHERE trade_date >= '{start_date}'"

    sql = f"""
    SELECT
      trade_date,
      ticker,
      adj_close,
      return_1d,
      ndx_price_ratio,
      fear_greed
    FROM `mag7_intel_mart.market_sentiment_ts`
    {where_clause}
    ORDER BY trade_date, ticker
    """
    return run_query(sql)

# ---------------------------------------------------------------------
# Market Sentiment Loaders - for Pong page
# ---------------------------------------------------------------------
@st.cache_data(ttl=600)
def load_price_macro(
    tickers: list[str],
    start_date: str,   # 'YYYY-MM-DD'
    end_date: str,     # 'YYYY-MM-DD'
    prices_table: str = TABLE_FACT_PRICE_FEATS,
    macro_table: str = TABLE_FACT_MACRO,
) -> pd.DataFrame:
    """
    Loads price + macro sentiment for selected tickers + date range.
    Uses run_query() to keep the app consistent (no bigquery.Client here).
    """

    if not tickers:
        return pd.DataFrame()

    tickers_sql = ", ".join([f"'{t}'" for t in tickers])

    sql = f"""
    SELECT
      p.trade_date,
      p.ticker,
      p.open,
      p.high,
      p.low,
      p.adj_close,
      p.volume,
      p.fwd_return_1d,
      p.fwd_return_5d,
      p.fwd_return_10d,
      p.fwd_return_20d,
      p.ma_20,
      p.ma_50,
      p.ma_200,
      s.fear_greed,
      s.mkt_sp500,
      s.mkt_sp125,
      s.stock_strength,
      s.stock_breadth,
      s.put_call,
      s.volatility,
      s.volatility_50,
      s.safe_haven,
      s.junk_bonds
    FROM `{prices_table}` p
    LEFT JOIN `{macro_table}` s
      ON p.trade_date = s.trade_date
    WHERE p.ticker IN ({tickers_sql})
      AND p.trade_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    ORDER BY p.trade_date ASC, p.ticker ASC
    """

    df = run_query(sql)
    if df is None or df.empty:
        return pd.DataFrame()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df

@st.cache_data(ttl=3600)
def load_available_tickers(prices_table: str = TABLE_FACT_PRICE_FEATS) -> list[str]:
    sql = f"SELECT DISTINCT ticker FROM `{prices_table}` ORDER BY ticker"
    df = run_query(sql)
    if df is None or df.empty:
        return []
    return df["ticker"].tolist()


@st.cache_data(ttl=3600)
def load_date_bounds(prices_table: str = TABLE_FACT_PRICE_FEATS) -> tuple[str, str]:
    sql = f"""
    SELECT
      CAST(MIN(trade_date) AS STRING) AS min_date,
      CAST(MAX(trade_date) AS STRING) AS max_date
    FROM `{prices_table}`
    """
    df = run_query(sql)
    if df is None or df.empty:
        return ("2000-01-01", "2000-01-01")
    return (df.loc[0, "min_date"], df.loc[0, "max_date"])

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
def load_market_sentiment_history(
    ticker: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    where = [f"ticker = '{ticker}'"]
    if start_date:
        where.append(f"trade_date >= DATE('{start_date}')")
    if end_date:
        where.append(f"trade_date <= DATE('{end_date}')")

    sql = f"""
    SELECT *
    FROM `{TABLE_MART_MARKET_SENTIMENT_TS}`
    WHERE {" AND ".join(where)}
    ORDER BY trade_date
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
