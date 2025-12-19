import os
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------

# Resolve project root (monorepo root)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

env_file = PROJECT_ROOT / ".env"
if env_file.exists():
    load_dotenv(env_file)

# ---------------------------------------------------------------------
# GCP / BigQuery Settings
# ---------------------------------------------------------------------

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
# BQ_LOCATION   = os.getenv("GCP_REGION", "US")
GOOGLE_APPLICATION_CREDENTIALS   = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BQ_DATASET_CORE = os.getenv("BQ_DATASET_CORE", "mag7_intel_core")
BQ_DATASET_MART = os.getenv("BQ_DATASET_MART", "mag7_intel_mart")

if not GCP_PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID is not set (check .env or environment).")

# ---------------------------------------------------------------------
# Canonical Table References
# ---------------------------------------------------------------------

TABLE_FACT_PRICES      = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_prices"
TABLE_FACT_PRICE_FEATS = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_price_features"
TABLE_FACT_REGIMES     = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_regimes"
TABLE_FACT_SENTIMENT   = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_ticker_sentiment_daily"
TABLE_FACT_MACRO       = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.fact_macro_sentiment_daily"

TABLE_DIM_TICKER       = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.dim_ticker"
TABLE_DIM_CALENDAR     = f"{GCP_PROJECT_ID}.{BQ_DATASET_CORE}.dim_calendar"

# ---------------------------------------------------------------------
# Mart Tables
# ---------------------------------------------------------------------

TABLE_S0_CORE_VALUE        = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.s0_core_value"
TABLE_S0_RESEARCH_EVENTS   = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.s0_research_events"
TABLE_S0_RESEARCH_PERF     = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.s0_research_performance"
TABLE_S1_CORE_MOMREV       = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.s1_core_momrev"

TABLE_MART_RISK                = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.risk_summary"
TABLE_MART_MACRO_RISK_TS       = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.macro_risk_ts"
TABLE_MART_REGIME_SUMMARY      = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.regime_summary"
TABLE_MART_TICKER_OVERVIEW     = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.research_ticker_profile"
TABLE_MART_PRICE_SUMMARY       = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.price_summary"
TABLE_MART_MARKET_SENTIMENT_TS = f"{GCP_PROJECT_ID}.{BQ_DATASET_MART}.market_sentiment_ts"
# ---------------------------------------------------------------------
# App Defaults
# ---------------------------------------------------------------------

DEFAULT_START_DATE = os.getenv("START_DATE", "2020-01-01")
