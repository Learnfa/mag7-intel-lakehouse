{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 's0_core_value',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ["ticker"],
    tags         = ['mart', 'signal', 'core', 'value']
) }}

-- ---------------------------------------------------------------------
-- s0_signal_core_value
--
-- Canonical core alpha signal by bucket value only.
--
-- Defines value-style long setup using:
--   - regime_bucket_10 <= 3 AND zscore_bucket_10 <= 3
-- Outputs:
--   - LONG_SETUP / OVEREXTENDED / NEUTRAL--
--
-- No volatility/macro/exec logic here.
-- ---------------------------------------------------------------------

WITH base AS (
    SELECT
        trade_date,
        ticker,

        -- regime & position metrics
        regime_bucket_10,
        zscore_bucket_10,
        price_pos_200d,
        price_zscore_20d,

        CURRENT_TIMESTAMP() AS asof_ts,
        'S0_value_v1' AS signal_version
    FROM {{ ref('fact_regimes') }}
),

labeled AS (
    SELECT
        trade_date,
        ticker,
        regime_bucket_10,
        zscore_bucket_10,
        price_pos_200d,
        price_zscore_20d,
        asof_ts,
        signal_version,

        -- State flags
        (regime_bucket_10 IS NOT NULL AND zscore_bucket_10 IS NOT NULL) AS has_buckets,

        CASE
            WHEN regime_bucket_10 <= 3 AND zscore_bucket_10 <= 3 THEN TRUE
            ELSE FALSE
        END AS is_long_setup,

        CASE
            WHEN regime_bucket_10 >= 8 AND zscore_bucket_10 >= 8 THEN TRUE
            ELSE FALSE
        END AS is_overextended,

        -- Human-readable state
        CASE
            WHEN regime_bucket_10 IS NULL OR zscore_bucket_10 IS NULL THEN 'MISSING'
            WHEN regime_bucket_10 <= 3 AND zscore_bucket_10 <= 3 THEN 'LONG_SETUP'
            WHEN regime_bucket_10 >= 8 AND zscore_bucket_10 >= 8 THEN 'OVEREXTENDED'
            ELSE 'NEUTRAL'
        END AS core_signal_state,

        -- Explainability: why not long (or why missing)
        CASE
            WHEN regime_bucket_10 IS NULL OR zscore_bucket_10 IS NULL THEN 'missing_buckets'
            WHEN regime_bucket_10 > 3 AND zscore_bucket_10 > 3 THEN 'both_not_cheap'
            WHEN regime_bucket_10 > 3 THEN 'regime_not_cheap'
            WHEN zscore_bucket_10 > 3 THEN 'zscore_not_cheap'
            ELSE 'ok'
        END AS core_reason,

        -- Ranking score (bounded 0..6) for "cheapness"
        -- 6 = (bucket 1, bucket 1), 0 = not attractive
        CASE
            WHEN regime_bucket_10 IS NULL OR zscore_bucket_10 IS NULL THEN 0
            ELSE GREATEST(0, (4 - regime_bucket_10) + (4 - zscore_bucket_10))
        END AS core_score,

        -- Normalized score 0..1 (useful for Streamlit sliders)
        CASE
            WHEN regime_bucket_10 IS NULL OR zscore_bucket_10 IS NULL THEN 0.0
            ELSE SAFE_DIVIDE(
                GREATEST(0, (4 - regime_bucket_10) + (4 - zscore_bucket_10)),
                6.0
            )
        END AS core_score_norm,

        -- Descriptive label
        CASE
            WHEN regime_bucket_10 IS NULL THEN 'Unknown'
            WHEN regime_bucket_10 <= 3 THEN 'Cheap'
            WHEN regime_bucket_10 >= 8 THEN 'Expensive'
            ELSE 'Fair'
        END AS regime_label

    FROM base
)

SELECT *
FROM labeled
