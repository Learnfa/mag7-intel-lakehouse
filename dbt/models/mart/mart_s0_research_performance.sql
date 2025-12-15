{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 's0_research_performance',
    cluster_by   = ['ticker', 'period_label', 'horizon'],
    tags         = ['mart', 'research', 'signal', 'performance']
) }}

-- ---------------------------------------------------------------------
-- s0_research_performance
--
-- Research validation table (NOT execution backtest).
-- Uses forward returns already computed in fact_regimes (research-only fields).
--
-- Provides:
--  - FULL vs EARLY vs LATE split (first 50% dates vs last 50% dates per ticker)
--  - conditioned stats by core_signal_state
--  - horizons: 5d, 10d, 20d
--
-- Purpose:
--  - power Streamlit "Research & Validation" pages (heatmaps, robustness tables)
--  - avoid heavy computation in Python/UI
--
-- Explicitly NOT included:
--  - trading assumptions (entry lag, holding mechanics, position sizing)
--  - cumulative equity curve logic
-- ---------------------------------------------------------------------

WITH core AS (
    SELECT
        trade_date,
        ticker,
        core_signal_state,
        is_long_setup,
        core_score,
        regime_bucket_10,
        zscore_bucket_10
    FROM {{ ref('mart_s0_core_value') }}
),

reg AS (
    SELECT
        trade_date,
        ticker,
        -- forward returns (research-only)
        fwd_return_1d,
        fwd_return_5d,
        fwd_return_10d,
        fwd_return_20d
    FROM {{ ref('fact_regimes') }}
),

joined AS (
    SELECT
        c.trade_date,
        c.ticker,
        c.core_signal_state,
        c.is_long_setup,
        c.core_score,
        c.regime_bucket_10,
        c.zscore_bucket_10,

        r.fwd_return_1d,
        r.fwd_return_5d,
        r.fwd_return_10d,
        r.fwd_return_20d,

        -- Early/Late split per ticker (by date order)
        CASE
            WHEN NTILE(2) OVER (PARTITION BY c.ticker ORDER BY c.trade_date) = 1 THEN 'EARLY'
            ELSE 'LATE'
        END AS period_label
    FROM core c
    INNER JOIN reg r
        ON c.trade_date = r.trade_date
       AND c.ticker     = r.ticker
),

-- normalize horizons into rows
long_horizon AS (
    SELECT
        trade_date,
        ticker,
        period_label,
        core_signal_state,
        is_long_setup,
        core_score,
        regime_bucket_10,
        zscore_bucket_10,
        5 AS horizon,
        fwd_return_5d AS forward_return
    FROM joined
    WHERE fwd_return_5d IS NOT NULL

    UNION ALL

    SELECT
        trade_date,
        ticker,
        period_label,
        core_signal_state,
        is_long_setup,
        core_score,
        regime_bucket_10,
        zscore_bucket_10,
        10 AS horizon,
        fwd_return_10d AS forward_return
    FROM joined
    WHERE fwd_return_10d IS NOT NULL

    UNION ALL

    SELECT
        trade_date,
        ticker,
        period_label,
        core_signal_state,
        is_long_setup,
        core_score,
        regime_bucket_10,
        zscore_bucket_10,
        20 AS horizon,
        fwd_return_20d AS forward_return
    FROM joined
    WHERE fwd_return_20d IS NOT NULL
),

agg_period AS (
    -- EARLY/LATE
    SELECT
        ticker,
        period_label,
        horizon,
        core_signal_state,

        COUNT(*) AS n_obs,
        COUNTIF(is_long_setup) AS n_long_setup_obs,

        AVG(forward_return) AS avg_forward_return,
        APPROX_QUANTILES(forward_return, 2)[OFFSET(1)] AS median_forward_return,
        STDDEV_SAMP(forward_return) AS std_forward_return,
        COUNTIF(forward_return > 0) / COUNT(*) AS win_rate
    FROM long_horizon
    GROUP BY 1,2,3,4
),

agg_full AS (
    -- FULL (no split)
    SELECT
        ticker,
        'FULL' AS period_label,
        horizon,
        core_signal_state,

        COUNT(*) AS n_obs,
        COUNTIF(is_long_setup) AS n_long_setup_obs,

        AVG(forward_return) AS avg_forward_return,
        APPROX_QUANTILES(forward_return, 2)[OFFSET(1)] AS median_forward_return,
        STDDEV_SAMP(forward_return) AS std_forward_return,
        COUNTIF(forward_return > 0) / COUNT(*) AS win_rate
    FROM long_horizon
    GROUP BY 1,2,3,4
)

SELECT * FROM agg_period
UNION ALL
SELECT * FROM agg_full
