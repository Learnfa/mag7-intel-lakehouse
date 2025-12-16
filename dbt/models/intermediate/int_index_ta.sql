{{ config( 
    materialized = 'incremental',
    schema = 'intermediate',
    alias = 'index_ta',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by = ["ticker"],
    incremental_strategy = 'insert_overwrite',
    tags = ["intermediate", "ta", "index"]
) }}

-- 1. Get base index prices from staging table
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
  FROM {{ ref('stg_index') }}
  WHERE trade_date IS NOT NULL
  {% if is_incremental() %}
    -- recompute enough history for 252d lookbacks + enough future for fwd returns
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 450 DAY)
  {% endif %}
),

-- 2. Add lags needed for returns & ATR calc
lagged AS (
  SELECT
    *,
    LAG(adj_close, 1)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_1,
    LAG(adj_close, 5)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_5,
    LAG(adj_close, 10) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_10,
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
    SAFE_DIVIDE(adj_close - prev_adj_close_10, prev_adj_close_10) AS return_10d,
    SAFE_DIVIDE(adj_close - prev_adj_close_20, prev_adj_close_20) AS return_20d,

    -- True range for ATR
    GREATEST(
      high - low,
      ABS(high - prev_close),
      ABS(low  - prev_close)
    ) AS true_range
  FROM lagged
),

-- 4. Add *forward* prices for forward-return calculations
forward AS (
  SELECT
    *,
    LEAD(adj_close,  1) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_1,
    LEAD(adj_close,  5) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_5,
    LEAD(adj_close, 10) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_10,
    LEAD(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_20
  FROM returns
)

-- 5. Construct final table with rolling/window TA features + forward returns
SELECT
  trade_date,
  ticker,
  open,
  high,
  low,
  close,
  adj_close,
  volume,

  -- === Backward returns (historical) ===
  return_1d,
  return_5d,
  return_10d,
  return_20d,

  -- === Forward returns (for alpha / regime analysis) ===
  SAFE_DIVIDE(next_adj_close_1  - adj_close, adj_close) AS fwd_return_1d,
  SAFE_DIVIDE(next_adj_close_5  - adj_close, adj_close) AS fwd_return_5d,
  SAFE_DIVIDE(next_adj_close_10 - adj_close, adj_close) AS fwd_return_10d,
  SAFE_DIVIDE(next_adj_close_20 - adj_close, adj_close) AS fwd_return_20d,

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

  -- === Rolling price levels (200-day high/low) ===
  MAX(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS rolling_max_200d,

  MIN(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS rolling_min_200d,

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
  ) AS ma_200       -- long-term

FROM forward
