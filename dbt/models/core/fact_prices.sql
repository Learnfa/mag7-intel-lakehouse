{{ config(
    materialized = 'table',
    schema       = 'core',
    alias        = 'fact_prices',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ['ticker'],
    tags         = ['core', 'fact', 'prices']
) }}

-- Thin price fact:
--   - keep only OHLCV, ajd_close for mag7 only
--   - filter out inactive tickers based on dim_ticker

WITH base AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    return_1d
  FROM {{ ref('int_mag7_ta') }}  -- mag7 only
),

filtered AS (
  SELECT b.*
  FROM base b
  LEFT JOIN {{ source('core', 'dim_ticker') }} dt
    ON b.ticker = dt.ticker
  WHERE COALESCE(dt.is_active, 1) = 1
)

SELECT * FROM filtered
