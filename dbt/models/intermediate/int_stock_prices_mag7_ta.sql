{{ config( 
    materialized = 'table',
    schema = 'intermediate',
    alias = 'stock_prices_mag7_ta',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by = ["ticker"],
    tags = ["intermediate", "ta", "mag7"]
) }}

-- 1. Get base prices from staging table
WITH prices AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume
  FROM {{ source('staging', 'stock_prices_mag7') }}
),

-- 2. Add lags needed for returns & ATR calc
lagged AS (
  SELECT
    *,
    LAG(adj_close, 1)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_1,
    LAG(adj_close, 5)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_5,
    LAG(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_20,
    LAG(close, 1)      OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close
  FROM prices
),

-- 3. Compute point-in-time metrics: returns, true range (for ATR)
returns AS (
  SELECT
    *,
    -- 1-day, 5-day & 20-day returns
    SAFE_DIVIDE(adj_close - prev_adj_close_1, prev_adj_close_1) AS return_1d,
    SAFE_DIVIDE(adj_close - prev_adj_close_5,  prev_adj_close_5)  AS return_5d,
    SAFE_DIVIDE(adj_close - prev_adj_close_20, prev_adj_close_20) AS return_20d,

    -- True range for ATR
    GREATEST(
      high - low,
      ABS(high - prev_close),
      ABS(low  - prev_close)
    ) AS true_range
  FROM lagged
)

-- 4. Construct final table with rolling/window TA features
SELECT
  trade_date,
  ticker,
  open,
  high,
  low,
  close,
  adj_close,
  volume,

  -- === Returns ===
  return_1d,
  return_5d,
  return_20d,

  -- === Rolling volatility (stddev of daily returns) ===
  STDDEV_SAMP(return_1d) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS vola_20d,

  STDDEV_SAMP(return_1d) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
  ) AS vola_60d,

  -- === Cumulative return over ~1 month (sum of daily returns over 20 days) ===
  SUM(return_1d) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS cumsum_return_20d,

  -- === Rolling price levels (20-day high/low) ===
  MAX(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS rolling_max_20d,

  MIN(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS rolling_min_20d,

  -- === Price z-score vs 20-day window ===
  (
    adj_close
    - AVG(adj_close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
      )
  )
  /
  NULLIF(
    STDDEV_SAMP(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ),
    0
  ) AS price_zscore_20d,

  -- === ATR(14) ===
  AVG(true_range) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
  ) AS atr_14,

  -- === Moving averages (MAs) ===
  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS ma_20,       -- ~1 trading month

  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
  ) AS ma_50,       -- mid-term

  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS ma_200,      -- long-term

FROM returns
