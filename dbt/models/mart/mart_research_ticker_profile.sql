{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'research_ticker_profile',
    tags         = ['mart', 'ticker', 'overview']
) }}

-- 1) Base data from regime fact table
WITH base AS (
    SELECT
        r.trade_date,
        r.ticker,

        -- Forward returns (from fact_regimes)
        r.fwd_return_1d,
        r.fwd_return_5d,
        r.fwd_return_10d,
        r.fwd_return_20d,

        -- Regime / style (from fact_regimes)
        r.regime_bucket_10,
        r.combined_regime_style,

        -- Optional z-score regime fields (from fact_regimes)
        r.zscore_bucket_10,
        r.zscore_regime_5,

        -- Volatility + trend (from fact_prices)
        p.vola_20d,
        p.vola_60d,
        p.ma_20,
        p.ma_50,
        p.ma_200,

        -- Benchmark-relative (from fact_prices; equity rows only, NULL for index rows)
        p.ndx_excess_return_1d,
        p.ndx_excess_return_5d,
        p.ndx_excess_return_10d,
        p.ndx_excess_return_20d,

        p.ndxe_excess_return_1d,
        p.ndxe_excess_return_5d,
        p.ndxe_excess_return_10d,
        p.ndxe_excess_return_20d

    FROM {{ ref('fact_regimes') }} r
    LEFT JOIN {{ ref('int_mag7_ta_benchmark') }} p
      ON p.trade_date = r.trade_date
     AND p.ticker     = r.ticker
),

-- 2) Summary metrics per ticker
agg AS (
    SELECT
        ticker,

        -- ===== Absolute forward return averages =====
        AVG(fwd_return_1d)  AS avg_fwd_1d,
        AVG(fwd_return_5d)  AS avg_fwd_5d,
        AVG(fwd_return_10d) AS avg_fwd_10d,
        AVG(fwd_return_20d) AS avg_fwd_20d,

        -- ===== Excess forward returns (NDX) =====
        AVG(ndx_excess_return_1d)  AS avg_ndx_excess_1d,
        AVG(ndx_excess_return_5d)  AS avg_ndx_excess_5d,
        AVG(ndx_excess_return_10d) AS avg_ndx_excess_10d,
        AVG(ndx_excess_return_20d) AS avg_ndx_excess_20d,

        -- ===== Excess forward returns (NDXE) =====
        AVG(ndxe_excess_return_1d)  AS avg_ndxe_excess_1d,
        AVG(ndxe_excess_return_5d)  AS avg_ndxe_excess_5d,
        AVG(ndxe_excess_return_10d) AS avg_ndxe_excess_10d,
        AVG(ndxe_excess_return_20d) AS avg_ndxe_excess_20d,

        -- ===== Volatility =====
        AVG(vola_20d) AS avg_vola_20d,
        AVG(vola_60d) AS avg_vola_60d,

        -- ===== Trend strength =====
        AVG(ma_20)  AS avg_ma_20,
        AVG(ma_50)  AS avg_ma_50,
        AVG(ma_200) AS avg_ma_200,

        -- ===== Regime distribution (bucket 10) =====
        SAFE_DIVIDE(COUNTIF(regime_bucket_10 BETWEEN 1 AND 3),  COUNT(*)) AS pct_regime_value,
        SAFE_DIVIDE(COUNTIF(regime_bucket_10 BETWEEN 4 AND 7),  COUNT(*)) AS pct_regime_neutral,
        SAFE_DIVIDE(COUNTIF(regime_bucket_10 BETWEEN 8 AND 10), COUNT(*)) AS pct_regime_momentum,

        -- ===== Combined style distribution =====
        SAFE_DIVIDE(COUNTIF(combined_regime_style = 'deep_value'),   COUNT(*)) AS pct_deep_value,
        SAFE_DIVIDE(COUNTIF(combined_regime_style = 'value_setup'),  COUNT(*)) AS pct_value_setup,
        SAFE_DIVIDE(COUNTIF(combined_regime_style = 'momentum'),     COUNT(*)) AS pct_momentum,
        SAFE_DIVIDE(COUNTIF(combined_regime_style = 'overextended'), COUNT(*)) AS pct_overextended,

        COUNT(*) AS n_observations

    FROM base
    GROUP BY ticker
),

-- 3) Attach metadata from dim_ticker (sector, company_name, etc.)
final AS (
    SELECT
        a.*,
        d.company_name,
        d.sector,
        d.industry,
        d.exchange
    FROM agg a
    LEFT JOIN {{ source('core', 'dim_ticker') }} d
        ON a.ticker = d.ticker
)

SELECT *
FROM final
ORDER BY ticker
