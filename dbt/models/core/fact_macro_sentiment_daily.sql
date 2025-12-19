{{ config(
    materialized         = 'incremental',
    schema               = 'core',
    alias                = 'fact_macro_sentiment_daily',
    partition_by         = { "field": "trade_date", "data_type": "date" },
    incremental_strategy = 'insert_overwrite',
    cluster_by           = ['trade_date'],
    tags                 = ['core', 'fact', 'macro', 'sentiment']
) }}

-- 1) Base daily FNG data from staging
WITH base AS (
    SELECT
        fng_date AS trade_date,
        fear_greed,
        mkt_sp500,
        mkt_sp125,
        stock_strength,
        stock_breadth,
        put_call,
        volatility,
        volatility_50,
        safe_haven,
        junk_bonds,
        fetched_at
    FROM {{ ref('stg_fng') }}
    WHERE fng_date IS NOT NULL
    {% if is_incremental() %}
      AND fng_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}

),

-- 2) Add lags for rate-of-change (ROC) calculations
lagged AS (
    SELECT
        *,
        LAG(fear_greed,   1) OVER (ORDER BY trade_date) AS prev_fng_1d,
        LAG(fear_greed,   5) OVER (ORDER BY trade_date) AS prev_fng_5d,
        LAG(fear_greed,  20) OVER (ORDER BY trade_date) AS prev_fng_20d,

        LAG(volatility,   1) OVER (ORDER BY trade_date) AS prev_vol_1d,
        LAG(volatility,   5) OVER (ORDER BY trade_date) AS prev_vol_5d,
        LAG(volatility,  20) OVER (ORDER BY trade_date) AS prev_vol_20d
    FROM base
),

-- 3) Rolling stats computed ONCE (20d + 60d)
roll AS (
    SELECT
        l.*,

        -- ===== 20d rolling mean & std =====
        AVG(fear_greed)  OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS fear_greed_mean_20d,
        STDDEV_SAMP(fear_greed) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS fear_greed_std_20d,

        AVG(volatility)  OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_mean_20d,
        STDDEV_SAMP(volatility) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volatility_std_20d,

        AVG(put_call)    OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS put_call_mean_20d,
        STDDEV_SAMP(put_call) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS put_call_std_20d,

        AVG(safe_haven)  OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS safe_haven_mean_20d,
        STDDEV_SAMP(safe_haven) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS safe_haven_std_20d,

        -- ===== 60d rolling mean & std =====
        AVG(fear_greed)  OVER (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS fear_greed_mean_60d,
        STDDEV_SAMP(fear_greed) OVER (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS fear_greed_std_60d,

        AVG(volatility)  OVER (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS volatility_mean_60d,
        STDDEV_SAMP(volatility) OVER (ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS volatility_std_60d

    FROM lagged l
),

-- 4) Z-scores computed from roll fields (no repeated windows)
zs AS (
    SELECT
        r.*,

        SAFE_DIVIDE(r.fear_greed  - r.fear_greed_mean_20d,  NULLIF(r.fear_greed_std_20d, 0))  AS fear_greed_z_20d,
        SAFE_DIVIDE(r.volatility  - r.volatility_mean_20d,  NULLIF(r.volatility_std_20d, 0))  AS volatility_z_20d,
        SAFE_DIVIDE(r.put_call    - r.put_call_mean_20d,    NULLIF(r.put_call_std_20d, 0))    AS put_call_z_20d,
        SAFE_DIVIDE(r.safe_haven  - r.safe_haven_mean_20d,  NULLIF(r.safe_haven_std_20d, 0))  AS safe_haven_z_20d

    FROM roll r
),

-- 5) Final select with ROC + composite risk-off score using z-scores
final AS (
    SELECT
        trade_date,

        -- ===== Raw macro features (CNN Fear & Greed) =====
        fear_greed,
        mkt_sp500,
        mkt_sp125,
        stock_strength,
        stock_breadth,
        put_call,
        volatility,
        volatility_50,
        safe_haven,
        junk_bonds,
        fetched_at,

        -- ===== Rate of change (ROC) =====
        SAFE_DIVIDE(fear_greed - prev_fng_1d,  prev_fng_1d)  AS fear_greed_roc_1d,
        SAFE_DIVIDE(fear_greed - prev_fng_5d,  prev_fng_5d)  AS fear_greed_roc_5d,
        SAFE_DIVIDE(fear_greed - prev_fng_20d, prev_fng_20d) AS fear_greed_roc_20d,

        SAFE_DIVIDE(volatility - prev_vol_1d,  prev_vol_1d)  AS volatility_roc_1d,
        SAFE_DIVIDE(volatility - prev_vol_5d,  prev_vol_5d)  AS volatility_roc_5d,
        SAFE_DIVIDE(volatility - prev_vol_20d, prev_vol_20d) AS volatility_roc_20d,

        -- ===== Rolling stats (exposed for downstream marts) =====
        fear_greed_mean_20d,
        fear_greed_std_20d,
        volatility_mean_20d,
        volatility_std_20d,
        put_call_mean_20d,
        put_call_std_20d,
        safe_haven_mean_20d,
        safe_haven_std_20d,

        fear_greed_mean_60d,
        fear_greed_std_60d,
        volatility_mean_60d,
        volatility_std_60d,

        -- ===== Z-scores (20d window) =====
        fear_greed_z_20d,
        volatility_z_20d,
        put_call_z_20d,
        safe_haven_z_20d,

        -- ===== Composite "risk-off" score (20d) =====
        -- Invert Fear & Greed z-score: lower fear_greed => more risk-off
        (
          COALESCE(-1 * fear_greed_z_20d, 0)
          + COALESCE(volatility_z_20d, 0)
          + COALESCE(put_call_z_20d, 0)
          + COALESCE(safe_haven_z_20d, 0)
        ) / 4.0 AS macro_risk_off_score_20d

    FROM zs
)

SELECT *
FROM final