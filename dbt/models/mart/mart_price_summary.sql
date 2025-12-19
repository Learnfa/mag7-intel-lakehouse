{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'price_summary',
    tags         = ['mart', 'price', 'overview']
) }}

-- 1) Base price facts (all assets: Mag7 + indices)
WITH base AS (
    SELECT
        ticker,
        trade_date,
        adj_close,
        volume,
        return_1d
    FROM {{ ref('fact_prices') }}
    WHERE return_1d IS NOT NULL
),

-- 2) Daily stats per ticker
per_ticker AS (
    SELECT
        ticker,

        -- ===== Level stats =====
        MIN(adj_close) AS min_price,
        MAX(adj_close) AS max_price,
        AVG(adj_close) AS avg_price,

        -- ===== Volume stats =====
        AVG(volume) AS avg_volume,
        APPROX_QUANTILES(volume, 100)[OFFSET(50)] AS p50_volume,

        -- ===== Return stats =====
        AVG(return_1d)                    AS avg_daily_return,
        STDDEV_SAMP(return_1d)            AS daily_volatility,

        -- Annualised approximations (252 trading days)
        POW(1 + AVG(return_1d), 252) - 1  AS annualized_return,
        STDDEV_SAMP(return_1d) * SQRT(252) AS annualized_volatility,

        COUNT(*) AS n_trading_days
    FROM base
    GROUP BY ticker
),

-- 3) Attach ticker metadata
final AS (
    SELECT
        p.ticker,
        d.company_name,
        d.sector,
        d.industry,
        d.exchange,

        p.min_price,
        p.max_price,
        p.avg_price,

        p.avg_volume,
        p.p50_volume,

        p.avg_daily_return,
        p.daily_volatility,
        p.annualized_return,
        p.annualized_volatility,
        p.n_trading_days
    FROM per_ticker p
    LEFT JOIN {{ source('core', 'dim_ticker') }} d
      ON p.ticker = d.ticker
)

SELECT *
FROM final
ORDER BY ticker
