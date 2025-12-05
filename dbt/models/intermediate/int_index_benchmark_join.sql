{{ config(
    materialized = 'view',
    schema = 'intermediate',
    alias = 'index_benchmark_join'
) }}

-- 1) Start from TA table
WITH ta AS (
  SELECT *
  FROM {{ ref('int_stock_prices_mag7_ta') }}
),

-- 2) Join to dim_ticker (seed) to bring in metadata + benchmark mapping
ta_with_dim AS (
  SELECT
    ta.*,
    CAST(dt.is_index AS BOOL)  AS is_index,
    dt.benchmark_ticker
  FROM ta
  LEFT JOIN {{ source('dim', 'dim_ticker') }} dt
    ON ta.ticker = dt.ticker
),

-- 3) Extract index rows only (these will act as the benchmarks)
index_ta AS (
  SELECT
    trade_date,
    ticker           AS index_ticker,
    return_1d        AS index_return_1d,
    ma_50            AS index_ma_50,
    ma_200           AS index_ma_200,
    vola_20d         AS index_vola_20d,
    price_zscore_20d AS index_price_zscore_20d
  FROM ta_with_dim
  WHERE is_index = TRUE
)

-- 4) Join each asset row to its benchmark index by date + benchmark_ticker
SELECT
  a.*,
  -- benchmark TA / returns
  i.index_ticker,
  i.index_return_1d,
  i.index_ma_50,
  i.index_ma_200,
  i.index_vola_20d,
  i.index_price_zscore_20d,
  -- key feature: excess return vs benchmark
  a.return_1d - i.index_return_1d AS excess_return_1d

FROM ta_with_dim a
LEFT JOIN index_ta i
  ON a.trade_date      = i.trade_date
 AND a.benchmark_ticker = i.index_ticker
