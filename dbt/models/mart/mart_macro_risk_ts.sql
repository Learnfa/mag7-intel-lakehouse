{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    schema       = 'mart',
    alias        = 'macro_risk_ts',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    tags         = ['mart', 'macro', 'risk', 'dashboard']
) }}

-- 1) Base macro fact
WITH base AS (
    SELECT
        *
    FROM {{ ref('fact_macro_sentiment_daily') }}
    WHERE trade_date IS NOT NULL
    {% if is_incremental() %}
      -- small window is fine for a thin fact
      AND trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    {% endif %}
),

-- 2) Add smoothed versions (5-day rolling averages of key scores)
smoothed AS (
    SELECT
        *,
        AVG(macro_risk_off_score_20d) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS macro_risk_off_score_5d,

        AVG(fear_greed_z_20d) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS fear_greed_z_5d,

        AVG(volatility_z_20d) OVER (
            ORDER BY trade_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volatility_z_5d
    FROM base
),

-- 3) Label macro regimes based on risk-off score & fear/greed
regimes AS (
    SELECT
        s.*,

        -- 4-bucket macro regime based on the composite risk-off score
        CASE
          WHEN macro_risk_off_score_20d >=  1.5 THEN 'panic'        -- extreme risk-off
          WHEN macro_risk_off_score_20d >=  0.5 THEN 'risk_off'     -- elevated stress
          WHEN macro_risk_off_score_20d <= -0.5 THEN 'risk_on'      -- complacent / bullish
          ELSE 'neutral'
        END AS macro_regime_4,

        -- A softer 3-bucket view if you want it for charts
        CASE
          WHEN macro_risk_off_score_20d >=  0.5 THEN 'risk_off'
          WHEN macro_risk_off_score_20d <= -0.5 THEN 'risk_on'
          ELSE 'neutral'
        END AS macro_regime_3,

        -- Decile bucket of the risk-off score (1 = most risk-on, 10 = most risk-off)
        NTILE(10) OVER (
            ORDER BY macro_risk_off_score_20d
        ) AS macro_risk_off_bucket_10
    FROM smoothed s
)

SELECT
    trade_date,

    -- Raw CNN Fear & Greed components
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

    -- Rolling stats / z-scores & composite from the core fact
    fear_greed_z_20d,
    volatility_z_20d,
    put_call_z_20d,
    safe_haven_z_20d,
    macro_risk_off_score_20d,

    -- Smoothed versions for nicer charts
    macro_risk_off_score_5d,
    fear_greed_z_5d,
    volatility_z_5d,

    -- Regime labels
    macro_regime_3,
    macro_regime_4,
    macro_risk_off_bucket_10
FROM regimes

