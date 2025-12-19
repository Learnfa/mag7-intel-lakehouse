{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    schema       = 'core',
    alias        = 'fact_regimes',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ['ticker', 'regime_bucket_10', 'zscore_bucket_10'],
    tags         = ['core', 'fact', 'regime']
) }}

-- Thin regime fact:
--   - regime labels (percentile, zscore, combined style)
--   - minimal inputs for explainability
--   - forward returns for outcome analysis

-- 1) Base: stock TA metrics with forward returns
WITH base AS (
  SELECT
    trade_date,
    ticker,

    -- inputs required to compute regimes
    adj_close,
    rolling_min_200d,
    rolling_max_200d,
    price_zscore_20d,

    -- forward returns – use for analyse per regime
    fwd_return_1d,
    fwd_return_5d,
    fwd_return_10d,
    fwd_return_20d

  FROM {{ ref('int_mag7_ta') }}
  WHERE trade_date IS NOT NULL
  {% if is_incremental() %}
    -- small window is fine for a thin fact
    AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}

),

-- 2) Percentile regime - Convert position in [min,max] to a 0-1 score, then to 10 buckets
pct_regime AS (
  SELECT
    *,
    -- position of today's price within its last-200-day range
    SAFE_DIVIDE(
      adj_close - rolling_min_200d,
      rolling_max_200d - rolling_min_200d
    ) AS price_pos_200d,

    -- convert to decile bucket 1..10 (1=cheapest vs its 200d range)
    CASE
      WHEN rolling_min_200d IS NULL
        OR rolling_max_200d IS NULL
        OR rolling_max_200d = rolling_min_200d
      THEN NULL
      ELSE LEAST(
        10,
        GREATEST(
          1,
          CAST(FLOOR(
            SAFE_DIVIDE(
              adj_close - rolling_min_200d,
              rolling_max_200d - rolling_min_200d
            ) * 10
          ) + 1 AS INT64)
        )
      )
    END AS regime_bucket_10
  FROM base
),

-- 3) Z-score decile regime (1 = most oversold, 10 = most overbought)
zscore_regime AS (
  SELECT
    *,
    -- NOTE: this is a "global" decile over the full history for each ticker
    NTILE(10) OVER (
      PARTITION BY ticker
      ORDER BY price_zscore_20d
    ) AS zscore_bucket_10,

    -- coarse 5-level version (time-stable + for dashboards)
    CASE
      WHEN price_zscore_20d <= -2 THEN 'deep_oversold'
      WHEN price_zscore_20d <= -1 THEN 'oversold'
      WHEN price_zscore_20d <   1 THEN 'neutral'
      WHEN price_zscore_20d <   2 THEN 'overbought'
      ELSE 'extreme_overbought'
    END AS zscore_regime_5

  FROM pct_regime
),

-- 4) Combined regime (percentile × z-score)
combined AS (
  SELECT
    *,
    CASE
      WHEN regime_bucket_10 <= 2 AND price_zscore_20d <= -1
        THEN 'deep_value'
      WHEN regime_bucket_10 <= 3 AND price_zscore_20d > -1 AND price_zscore_20d < 1
        THEN 'value_setup'
      WHEN regime_bucket_10 >= 8 AND price_zscore_20d BETWEEN -0.5 AND 0.5
        THEN 'momentum'
      WHEN regime_bucket_10 >= 8 AND price_zscore_20d >= 1
        THEN 'overextended'
      ELSE 'neutral'
    END AS combined_regime_style,

    -- simple banding: helps analysis + Streamlit legends (does not depend on MA/vol)
    CASE
      WHEN regime_bucket_10 BETWEEN 8 AND 10 THEN 'upper_decile'
      WHEN regime_bucket_10 BETWEEN 1 AND 3  THEN 'lower_decile'
      WHEN regime_bucket_10 IS NULL          THEN NULL
      ELSE 'mid'
    END AS pct_band_3

  FROM zscore_regime
)

SELECT
  trade_date,
  ticker,
  
  -- regime labels
    -- Percentile regime
  price_pos_200d,
  regime_bucket_10,
  pct_band_3,
  
  -- Z-score regime
  price_zscore_20d,
  zscore_bucket_10,
  zscore_regime_5,
  
  -- Combined regime
  combined_regime_style,
  
  -- regime explainability inputs
  rolling_min_200d,
  rolling_max_200d,
  
  -- outcomes  (alpha signals)
  fwd_return_1d,
  fwd_return_5d,
  fwd_return_10d,
  fwd_return_20d,

  -- simple outcome helpers (useful for win-rate stats in marts)
  (fwd_return_5d  > 0) AS fwd_win_5d,
  (fwd_return_10d > 0) AS fwd_win_10d,
  (fwd_return_20d > 0) AS fwd_win_20d

FROM combined
WHERE regime_bucket_10 IS NOT NULL
