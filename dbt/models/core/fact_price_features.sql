{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    schema       = 'core',
    alias        = 'fact_price_features',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ['ticker'],
    tags         = ['core', 'fact', 'prices']
) }}

-- Single source of truth:
-- int_mag7_ta_benchmark already includes:
--  - MAG7 rows enriched with benchmark-relative fields
--  - benchmark index rows (^NDX, ^NDXE) from int_index_ta

WITH base AS (
  SELECT
    *
  FROM {{ ref('int_mag7_ta_benchmark') }}
  WHERE trade_date IS NOT NULL
  {% if is_incremental() %}
    -- thin fact: only overwrite recent partitions
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
),

-- (Optional) enforce ticker universe via dim_ticker (active only)
filtered AS (
  SELECT
    b.*
  FROM base b
  LEFT JOIN {{ source('core', 'dim_ticker') }} dt
    ON b.ticker = dt.ticker
  WHERE COALESCE(dt.is_active, 1) = 1
)

SELECT
  trade_date,
  ticker,
  open,
  high,
  low,
  close,
  adj_close,
  volume,

  -- returns
  return_1d,
  return_5d,
  return_10d,
  return_20d,

  -- forward returns
  fwd_return_1d,
  fwd_return_5d,
  fwd_return_10d,
  fwd_return_20d,

  -- rolling / TA
  vola_20d,
  vola_60d,
  vola_z20d,
  vola_not_top_20_252d,
  vola_p80_252d,
  cumsum_return_20d,
  rolling_max_20d,
  rolling_min_20d,
  rolling_max_200d,
  rolling_min_200d,
  price_zscore_20d,
  atr_14,
  ma_12,
  ma_20,
  ma_26,
  ma_50,
  ma_100,
  ma_200,
  macd_signal_sma_9,
  rsi_14,
  rsi_avg_gain_14,
  rsi_avg_loss_14,
  bb_std_20d,
  bb_mid_20d,
  bb_upper_20d,
  bb_lower_20d,

  -- benchmark-relative (equity-only; NULL for index rows)
  ndx_excess_return_1d,
  ndx_excess_return_5d,
  ndx_excess_return_10d,
  ndx_excess_return_20d,
  ndx_relative_strength_20d,
  ndx_price_ratio,

  ndxe_excess_return_1d,
  ndxe_excess_return_5d,
  ndxe_excess_return_10d,
  ndxe_excess_return_20d,
  ndxe_relative_strength_20d,
  ndxe_price_ratio

FROM filtered
