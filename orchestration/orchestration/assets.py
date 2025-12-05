from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# This file: mag7_intel/orchestration/orchestration/assets.py
# Project root: mag7_intel/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MELTANO_DIR = PROJECT_ROOT / "meltano"
DBT_DIR = PROJECT_ROOT / "dbt"

load_dotenv(PROJECT_ROOT / ".env")


# 1) NEWS EXTRACTOR  --------------------------------------------------------- #
@asset(
    description=(
        "CSV files with Google News headlines and FinBERT sentiment for the "
        "configured tickers and window, written to data/news/ (or similar)."
    )
)
def news_csv(context: AssetExecutionContext) -> str:
    """
    Runs the news extractor and returns the path (or label) for the latest CSV - 
    run_news_extractor() writes a Meltano-friendly CSV under data/news/.
    """
    # Lazy import so Dagster can load even if src isn't on path in some contexts
    from src.extractors.news_extractor import run_news_extractor

    # Reasonable defaults – adjust to match your CLI behaviour
    from src.extractors.news_extractor import DEFAULT_TICKERS as tickers

    window = "1d"

    context.log.info(f"Running news extractor for tickers={tickers}, window={window}")
    csv_path = run_news_extractor(tickers, window)

    if not csv_path:
        context.log.warning(
            "run_news_extractor() did not return a path; using empty string."
        )
        return ""

    context.log.info(f"News extractor wrote CSV to: {csv_path}")
    return str(csv_path)


# 2) PRICES EXTRACTOR  ------------------------------------------------------- #

@asset(
    description=(
        "CSV files with daily OHLCV for Mag7, NASDAQ indexes (^IXIC, ^NDXE) and VIX, "
        "written to data/stocks/."
    )
)
def prices_csv(context: AssetExecutionContext) -> str:
    """
    Runs the stock extractor and returns the path to the last CSV file written.
    """
    from src.extractors.stocks_extractor import extract_to_csv

    # Defaults: daily incremental, Mag7 + indexes, include VIX
    mode = "incremental"
    universe = "mag7_with_indexes"
    include_vix = True

    # If your extract_to_csv requires tickers but supports None meaning "use default
    # universe/Mag7", this will work. Otherwise, pass the same Mag7 list as above.
    tickers = None

    context.log.info(
        f"Starting stock extractor (mode={mode}, universe={universe}, include_vix={include_vix})..."
    )

    csv_path = extract_to_csv(
        mode=mode,
        universe=universe,
        include_vix=include_vix,
        tickers=tickers,
    )

    if csv_path is None:
        context.log.warning("extract_to_csv() returned no data (csv_path=None).")
        return ""

    context.log.info(f"Stock extractor wrote CSV to: {csv_path}")
    return str(csv_path)


# 3) MELTANO LOAD (RAW → BigQuery)  ----------------------------------------- #

@asset(
    deps=[news_csv, prices_csv],
    description=(
        "Raw BigQuery tables loaded via Meltano from the news and stock CSVs "
        "(e.g. mag7_intel_raw.news_headlines, mag7_intel_raw.stock_prices)."
    ),
)
def raw_bq_loaded(context: AssetExecutionContext, news_csv: str, prices_csv: str) -> None:
    """
    Runs the Meltano job which reads both CSV streams and loads into BigQuery.

    Assumes your Meltano job 'load_csvs' is configured with:
      - news_headlines  stream from the news CSV(s)
      - stock_prices_all stream from the stock CSV(s)
    """
    context.log.info("Running Meltano job: load_csvs")
    context.log.info(f"news_csv asset: {news_csv}")
    context.log.info(f"prices_csv asset: {prices_csv}")

    result = subprocess.run(
        ["meltano", "run", "load_csvs"],
        cwd=MELTANO_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"Meltano run load_csvs failed with code {result.returncode}")


# 4) DBT TRANSFORMS (STAGING)  -------------------------------------- #

@asset(
    deps=[raw_bq_loaded],
    description=(
        "dbt models for staging and downstream marts, including:\n"
        "- stg_stock_prices_ (+ all/mag7/vix/index splits)\n"
        "- stg_news_headlines (with FinBERT sentiment)\n"
        "- any marts built on top of them."
    ),
)
def stg_stock_prices(context: AssetExecutionContext) -> None:
    """
    Runs dbt to materialize stg_stock_prices and everything downstream.

    Then split this into multiple dbt asset jobs later
    (e.g. one for staging, one for marts).
    """
    context.log.info("Running dbt: stg_stock_prices+")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "staging.*"
    ]
# "stg_stock_prices_all stg_stock_prices_mag7 stg_stock_prices_vix stg_stock_prices_index stg_news_headlines stg_gdelt_gkg_stock_news",

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")


# 5) DBT TRANSFORMS (INTERMEDIATE)  -------------------------------------- #

@asset(
    deps=[stg_stock_prices],
    description=(
        "dbt models for intermediate, including:\n"
        "- int_stock_prices_mag7_ta\n"
        "- int_index_benchmark_join"
    ),
)
def int_stock_prices_enrich(context: AssetExecutionContext) -> None:
    """
    Runs dbt to materialize stock_prices_ta and index_benchmark_join in the intermediate dataset.
    """
    context.log.info("Running dbt: int_stock_prices_ta, int_index_benchmark_join")

    dbt_cmd = [
        "dbt",
        "run",
        "-s",
        "intermediate.*"
    ]

    result = subprocess.run(
        dbt_cmd,
        cwd=DBT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")

