-- models/mart/mart_market_sentiment_ts.sql
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    schema       = 'mart',
    alias        = 'market_sentiment_ts',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ["ticker"],
    tags         = ['mart', 'market', 'sentiment', 'ts']
) }}

-- ---------------------------------------------------------------------
-- mart_market_sentiment_ts (THIN)
--
-- PURPOSE:
--   Primary time-series mart for Streamlit "Market & Sentiment" page.
--   Curated fields only (fast to load, stable contract).
--
-- GRAIN:
--   trade_date × ticker (equities only)
--
-- NOTES:
--   - No look-ahead / forward returns included.
--   - Benchmarks are pulled from index rows in fact_price_features.
--   - Macro series is forward-filled onto trading dates.
-- ---------------------------------------------------------------------

WITH price_base AS (
  SELECT
    trade_date,
    ticker,
    adj_close,
    volume,
    return_1d,
    return_5d,
    vola_20d,
    price_zscore_20d,
    ma_50,
    ma_200,
    -- keep ONE benchmark-relative feature for quick relative view
    ndx_price_ratio
  FROM {{ ref('fact_price_features') }}
  WHERE trade_date IS NOT NULL
  {% if is_incremental() %}
      -- small window is fine for a thin fact
      AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}

),

-- Benchmarks from the same fact (index rows)
bench AS (
  SELECT
    trade_date,
    MAX(IF(ticker = '^NDX',  adj_close, NULL)) AS ndx_adj_close,
    MAX(IF(ticker = '^NDXE', adj_close, NULL)) AS ndxe_adj_close
  FROM price_base
  GROUP BY trade_date
),

-- Keep only equities for this mart (exclude index rows like ^NDX)
equity_price AS (
  SELECT *
  FROM price_base
  WHERE NOT STARTS_WITH(ticker, '^')
),

sent_raw AS (
  SELECT
    trade_date,
    ticker,

    -- FinBERT (news)
    article_count,
    sentiment_mean,
    pos_count,
    neg_count,
    neu_count,

    -- GDELT (GKG)
    event_count,
    tone_mean
  FROM {{ ref('fact_ticker_sentiment_daily') }}
),

-- Normalize + smooth sentiment
sent_enriched AS (
  SELECT
    s.*,

    -- Canonical polarity rates (NULL-safe)
    SAFE_DIVIDE(COALESCE(pos_count, 0), NULLIF(article_count, 0)) AS finbert_pos_rate,
    SAFE_DIVIDE(COALESCE(neg_count, 0), NULLIF(article_count, 0)) AS finbert_neg_rate,
    SAFE_DIVIDE(COALESCE(neu_count, 0), NULLIF(article_count, 0)) AS finbert_neu_rate,
    SAFE_DIVIDE(COALESCE(pos_count, 0) - COALESCE(neg_count, 0), NULLIF(article_count, 0)) AS finbert_net_rate,

    -- Smooth for charts (7-trading-row MA)
    AVG(SAFE_DIVIDE(COALESCE(pos_count, 0) - COALESCE(neg_count, 0), NULLIF(article_count, 0))) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS finbert_net_ma7,

    AVG(sentiment_mean) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS finbert_sent_ma7,

    AVG(tone_mean) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS gdelt_tone_ma7,

    -- Lightweight “confidence” flags
    CASE WHEN COALESCE(article_count, 0) >= 5 THEN 1 ELSE 0 END AS finbert_has_enough_articles,
    CASE WHEN COALESCE(event_count, 0) >= 5 THEN 1 ELSE 0 END AS gdelt_has_enough_events

  FROM sent_raw s
),

macro_base AS (
  SELECT
    trade_date,
    macro_risk_off_score_20d,
    macro_risk_off_score_5d,
    fear_greed,
    macro_regime_4
  FROM {{ ref('mart_macro_risk_ts') }}
),

-- Align macro onto trading dates (forward-fill last available macro values)
calendar AS (
  SELECT DISTINCT trade_date
  FROM equity_price
),

macro_aligned AS (
  SELECT
    c.trade_date,

    LAST_VALUE(m.macro_risk_off_score_20d IGNORE NULLS) OVER (
      ORDER BY c.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS macro_risk_off_score_20d,

    LAST_VALUE(m.macro_risk_off_score_5d IGNORE NULLS) OVER (
      ORDER BY c.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS macro_risk_off_score_5d,

    LAST_VALUE(m.fear_greed IGNORE NULLS) OVER (
      ORDER BY c.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS fear_greed,

    LAST_VALUE(m.macro_regime_4 IGNORE NULLS) OVER (
      ORDER BY c.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS macro_regime_4

  FROM calendar c
  LEFT JOIN macro_base m
    ON c.trade_date = m.trade_date
)

SELECT
  p.trade_date,
  p.ticker,

  -- price (thin)
  p.adj_close,
  p.volume,
  p.return_1d,
  p.return_5d,
  p.vola_20d,
  p.price_zscore_20d,
  p.ma_50,
  p.ma_200,
  p.ndx_price_ratio,

  -- benchmark series (thin)
  b.ndx_adj_close,
  b.ndxe_adj_close,

  -- sentiment (raw + minimal)
  s.article_count,
  s.sentiment_mean,
  s.pos_count,
  s.neg_count,
  s.neu_count,
  s.event_count,
  s.tone_mean,

  -- sentiment (normalized + smoothed)
  s.finbert_pos_rate,
  s.finbert_neg_rate,
  s.finbert_neu_rate,
  s.finbert_net_rate,
  s.finbert_net_ma7,
  s.finbert_sent_ma7,
  s.gdelt_tone_ma7,
  s.finbert_has_enough_articles,
  s.gdelt_has_enough_events,

  -- macro (aligned)
  m.macro_regime_4,
  m.macro_risk_off_score_20d,
  m.macro_risk_off_score_5d,
  m.fear_greed

FROM equity_price p
LEFT JOIN bench b
  ON p.trade_date = b.trade_date
LEFT JOIN sent_enriched s
  ON p.trade_date = s.trade_date
 AND p.ticker     = s.ticker
LEFT JOIN macro_aligned m
  ON p.trade_date = m.trade_date
