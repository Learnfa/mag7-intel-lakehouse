#!/usr/bin/env python
"""
Stocks ingestor for Magnificent 7 using yfinance

Task:
    Decide date range (backfill vs incremental)
    Download prices from yfinance
    Write standardized CSV file for Meltano to load into BigQuery

Usage:
    python stocks_ingestor.py --mode backfill
    python stocks_ingestor.py --mode incremental

Env vars:
    TICKERS              (optional, comma-separated; default = Magnificent 7)
    START_DATE           (optional, for backfill; default: BACKFILL_YEARS ago, YYYY-MM-DD)
    BACKFILL_YEARS       (optional, default: 3)
    INCREMENTAL_DAYS     (optional, default: 1, for incremental mode)
    OUTPUT_DIR           (optional, default: "data/stocks")
"""

import argparse
import os
from datetime import date, datetime, timedelta
from typing import List
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
import yfinance as yf


# --------- Config ---------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

BASE_OUTPUT = os.getenv("OUTPUT_DIR", "./data")
BASE_OUTPUT_DIR = PROJECT_ROOT / BASE_OUTPUT

# Default tickers (Magnificent 7)
DEFAULT_TICKERS = [t.strip() for t in os.getenv("TICKERS", "").split(",") if t.strip()] or [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"
]


# --------- Helper Functions ---------

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def get_backfill_date_range() -> tuple[date, date]:
    """Determine backfill start/end dates based on START_DATE or BACKFILL_YEARS."""
    today = date.today()

    start_env = os.environ.get("START_DATE")
    if start_env:
        start = parse_date(start_env)
    else:
        years = int(os.environ.get("BACKFILL_YEARS", "3"))
        start = today - timedelta(days=365 * years)

    end = today  # inclusive logically
    return start, end


def get_incremental_date_range() -> tuple[date, date]:
    """
    Determine start/end dates for incremental load.

    Here we keep it simple and use the last N days based on INCREMENTAL_DAYS
    (default: 1 = today only). Idempotency / dedupe will be handled downstream
    in Meltano/dbt.
    """
    today = date.today()
    days = int(os.environ.get("INCREMENTAL_DAYS", "1"))
    # e.g. days=1 → start=today, end=today
    start = today - timedelta(days=days - 1)
    end = today
    return start, end


def download_prices(
    tickers: List[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Download OHLCV from yfinance for list of tickers between start and end (inclusive).
    yfinance's 'end' is exclusive, so we add +1 day.
    """
    end_exclusive = end + timedelta(days=1)
    data = yf.download(
        tickers=tickers,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        auto_adjust=False,
        group_by="ticker",
        progress=False,
    )

    if data.empty:
        return pd.DataFrame()

    # Normalize to one row per (date, ticker)
    if isinstance(data.columns, pd.MultiIndex):
        if data.columns.levels[0].isin(["Open", "High", "Low", "Close", "Adj Close", "Volume"]).any():
            data = data.swaplevel(axis=1)

        records = []
        for ticker in data.columns.levels[0]:
            df_t = data[ticker].copy()
            df_t["ticker"] = ticker
            df_t["date"] = df_t.index.date
            records.append(df_t.reset_index(drop=True))
        df = pd.concat(records, ignore_index=True)
    else:
        ticker = tickers[0]
        df = data.copy()
        df["ticker"] = ticker
        df["date"] = df.index.date
        df = df.reset_index(drop=True)

    # Rename columns to our schema expectations
    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename_map)

    # Keep only columns we care about
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = None

    df = df[keep_cols].dropna(subset=["date", "ticker"])

    # Ensure reasonable dtypes
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ticker"] = df["ticker"].astype(str)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    for col in ["open", "high", "low", "close", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ingested_at"] = datetime.utcnow().isoformat()

    return df


# --------- Core extractor module to be used by Dagster or main() ---------
def run_extractor(mode: str, tickers: List[str]) -> Path:
    """Core extraction logic, reusable from CLI or Dagster.

    Returns the path to the output CSV file.
    """
    if mode == "backfill":
        start, end = get_backfill_date_range()
    else:
        start, end = get_incremental_date_range()

    print(f"Mode: {mode}")
    print(f"Tickers: {tickers}")
    print(f"Date range: {start} → {end}")

    df = download_prices(tickers, start, end)
    if df.empty:
        print("No data returned from yfinance for this range; exiting.")
        return None

    output_dir = BASE_OUTPUT_DIR / "stocks"
    output_dir.mkdir(parents=True, exist_ok=True)

    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    output_path = output_dir / f"prices_{start_str}_{end_str}.csv"

    print(f"Writing {len(df)} rows to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Extract completed successfully.")

    return output_path


# --------- CLI Main entrypoint ---------
def main():
    parser = argparse.ArgumentParser(description="Extract Magnificent 7 stock prices via yfinance to CSV.")
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental"],
        required=True,
        help="backfill = historical load; incremental = recent N days only",
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=DEFAULT_TICKERS,
        help="Optional list of tickers to ingest (default=Magnificent 7).",
    )
    args = parser.parse_args()

    run_extractor(args.mode, args.tickers)

if __name__ == "__main__":
    main()
