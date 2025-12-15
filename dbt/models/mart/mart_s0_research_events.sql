{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 's0_research_events',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ["ticker"],
    tags         = ['mart', 'research', 'events']
) }}

-- ---------------------------------------------------------------------
-- signal_research_events
--
-- PURPOSE:
--   Row-level research dataset for visualization and exploration.
--
-- WARNING:
--   Contains look-ahead bias (LEAD).
--   NOT a trading backtest.
--   NOT to be used for execution logic.
-- ---------------------------------------------------------------------

WITH signals AS (
    SELECT * FROM {{ ref('mart_s0_core_value') }}
),

prices AS (
    SELECT 
        trade_date, 
        ticker, 
        adj_close
    FROM {{ ref('fact_prices') }}
),

combined AS (
    SELECT
        s.trade_date,
        s.ticker,
        s.core_signal_state,
        s.regime_bucket_10,
        s.zscore_bucket_10,
        p.adj_close
    FROM signals s
    INNER JOIN prices p 
        ON s.trade_date = p.trade_date 
        AND s.ticker = p.ticker
),

calculated AS (
    SELECT
        c.*,

        -- Forward returns (research only)
        (LEAD(c.adj_close, 5)  OVER (PARTITION BY c.ticker ORDER BY c.trade_date) - c.adj_close) / c.adj_close AS fwd_ret_5d,
        (LEAD(c.adj_close, 10) OVER (PARTITION BY c.ticker ORDER BY c.trade_date) - c.adj_close) / c.adj_close AS fwd_ret_10d,
        (LEAD(c.adj_close, 20) OVER (PARTITION BY c.ticker ORDER BY c.trade_date) - c.adj_close) / c.adj_close AS fwd_ret_20d,

        -- EARLY / LATE split (per ticker)
        CASE
            WHEN NTILE(2) OVER (PARTITION BY c.ticker ORDER BY c.trade_date) = 1
            THEN 'EARLY'
            ELSE 'LATE'
        END AS period_label

    FROM combined c
)

SELECT *
FROM calculated
WHERE fwd_ret_20d IS NOT NULL
