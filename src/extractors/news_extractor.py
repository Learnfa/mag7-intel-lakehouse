#!/usr/bin/env python
"""
News extractor for tickers using Google News RSS + FinBERT sentiment.

Usage:
    python src/extractor/news_extractor.py --window 1d
    python src/extractor/news_extractor.py --tickers AAPL MSFT --window 7d

Writes to:
    OUTPUT_DIR/news/news_<window>_<YYYYMMDD_HHMMSS>.csv

Columns:
    ticker
    headline
    timestamp
    url
    source          (google_news)
    sentiment_label (positive/negative/neutral)
    sentiment_score (float: pos_prob - neg_prob)
    window
    fetched_at
"""

import argparse
import os
from datetime import datetime
from typing import List, Dict
from pathlib import Path
import html

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

# ---------- Load .env ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

BASE_OUTPUT = os.getenv("OUTPUT_DIR", "./data")
BASE_OUTPUT_DIR = PROJECT_ROOT / BASE_OUTPUT

# Default tickers (Magnificent 7)
DEFAULT_TICKERS = [t.strip() for t in os.getenv("TICKERS", "").split(",") if t.strip()] or [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"
]

DEFAULT_WINDOW = "1d"   # overridden by --window

# ---------- FinBERT (financial sentiment) ----------

FINBERT_MODEL_NAME = "yiyanghkust/finbert-tone"

print("Loading FinBERT model for financial sentiment...")
_finbert_tokenizer = AutoTokenizer.from_pretrained(FINBERT_MODEL_NAME)
_finbert_model = AutoModelForSequenceClassification.from_pretrained(FINBERT_MODEL_NAME)
_finbert_model.eval()  # inference mode, no gradients

_id2label = _finbert_model.config.id2label  # e.g. {0: 'neutral', 1: 'positive', 2: 'negative'}


def add_finbert_sentiment(df: pd.DataFrame, text_col: str = "headline", batch_size: int = 16) -> pd.DataFrame:
    """
    Add FinBERT sentiment columns to the dataframe:
      - sentiment_label
      - sentiment_score (positive_prob - negative_prob)

    Assumes df[text_col] contains strings (headlines).
    """
    if df.empty:
        df["sentiment_label"] = []
        df["sentiment_score"] = []
        return df

    texts = df[text_col].fillna("").astype(str).tolist()
    labels: List[str] = []
    scores: List[float] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        inputs = _finbert_tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=128,
            return_tensors="pt",
        )

        with torch.no_grad():
            outputs = _finbert_model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1)

        for j in range(len(batch)):
            prob_vec = probs[j]
            label_id = int(torch.argmax(prob_vec))
            label = _id2label[label_id].lower()

            # derive a simple scalar score: positive_prob - negative_prob
            # (FinBERT usually has labels like neutral/positive/negative)
            # default indices in finbert-tone are:
            #   0: neutral, 1: positive, 2: negative
            pos_idx = None
            neg_idx = None
            for k, v in _id2label.items():
                if v.lower().startswith("pos"):
                    pos_idx = k
                elif v.lower().startswith("neg"):
                    neg_idx = k

            if pos_idx is not None and neg_idx is not None:
                score = float(prob_vec[pos_idx] - prob_vec[neg_idx])
            else:
                # fallback: use max prob as score
                score = float(prob_vec[label_id])

            labels.append(label)
            scores.append(score)

    df = df.copy()
    df["sentiment_label"] = labels
    df["sentiment_score"] = scores
    return df


# ---------- Helper Functions ----------

def build_google_news_url(ticker: str, window: str) -> str:
    """
    Build a Google News RSS URL for a given ticker.
    Supports time windows like: 1d, 7d, 30d, 90d (though >30d is best-effort).
    """
    query = f"{ticker} stock"
    if window:
        query += f" when:{window}"

    q_param = query.replace(" ", "+")
    return f"https://news.google.com/rss/search?q={q_param}&hl=en-US&gl=US&ceid=US:en"


def parse_pubdate(pubdate_raw: str) -> str:
    """Parse RSS pubDate to ISO-8601."""
    try:
        dt = datetime.strptime(pubdate_raw, "%a, %d %b %Y %H:%M:%S %Z")
        return dt.isoformat()
    except Exception:
        return pubdate_raw


def fetch_news_for_ticker(ticker: str, window: str) -> List[Dict]:
    url = build_google_news_url(ticker, window)
    print(f"Fetching news for {ticker} with window={window}:")
    print(f"  {url}")

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "xml")

    rows = []
    for item in soup.find_all("item"):
        title_tag = item.find("title")
        link_tag = item.find("link")
        pubdate_tag = item.find("pubDate")

        if not title_tag or not link_tag:
            continue

        headline = html.unescape(title_tag.get_text(strip=True))
        url_text = link_tag.get_text(strip=True)
        timestamp = parse_pubdate(pubdate_tag.get_text(strip=True)) if pubdate_tag else None

        rows.append(
            {
                "ticker": ticker,
                "headline": headline,
                "timestamp": timestamp,
                "url": url_text,
                "source": "google_news",
                "window": window,
                "fetched_at": datetime.utcnow().isoformat(),
            }
        )

    print(f"  Found {len(rows)} articles")
    return rows


def run_news_extractor(tickers: List[str], window: str) -> Path | None:
    all_records: List[Dict] = []

    for ticker in tickers:
        recs = fetch_news_for_ticker(ticker, window)
        all_records.extend(recs)

    if not all_records:
        print("No news fetched.")
        return None

    df = pd.DataFrame(all_records)

    # 🔥 Add FinBERT sentiment
    df = add_finbert_sentiment(df, text_col="headline")

    output_dir = BASE_OUTPUT_DIR / "news"
    output_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"news_{window}_{ts}.csv"

    print(f"Writing {len(df)} rows → {output_path}")
    df.to_csv(output_path, index=False)

    return output_path


# ---------- CLI Entry Point ----------

def main():
    parser = argparse.ArgumentParser(description="Extract ticker news via Google News RSS + FinBERT sentiment.")
    parser.add_argument(
        "--tickers",
        nargs="*",
        default=DEFAULT_TICKERS,
        help="Tickers to fetch news for."
    )
    parser.add_argument(
        "--window",
        default=DEFAULT_WINDOW,
        help="Time window: 1d, 7d, 30d (default: 1d)"
    )
    args = parser.parse_args()

    print(f"Tickers: {args.tickers}")
    print(f"Window: {args.window}")

    run_news_extractor(args.tickers, args.window)


if __name__ == "__main__":
    main()
