{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 'research_sentiment',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ["ticker", "sentiment_source"],
    tags         = ['mart', 'research', 'sentiment']
) }}

-- ---------------------------------------------------------------------
-- research_sentiment
--
-- PURPOSE:
--   Research-only sentiment vs forward returns dataset.
--   Contains LOOK-AHEAD bias (forward returns).
--   Not for trading or signal generation.
--
-- GRAIN:
--   trade_date × ticker × sentiment_source
-- ---------------------------------------------------------------------

WITH sentiment AS (
    SELECT
        trade_date,
        ticker,
        'FINBERT' AS sentiment_source,
        sentiment_mean AS sentiment_score
    FROM {{ ref('fact_ticker_sentiment_daily') }}
    WHERE sentiment_mean IS NOT NULL

    UNION ALL

    SELECT
        trade_date,
        ticker,
        'GDELT' AS sentiment_source,
        tone_mean AS sentiment_score
    FROM {{ ref('fact_ticker_sentiment_daily') }}
    WHERE tone_mean IS NOT NULL
),

prices AS (
    SELECT
        trade_date,
        ticker,
        fwd_return_5d,
        fwd_return_20d
    FROM {{ ref('int_mag7_ta') }}

),

joined AS (
    SELECT
        s.trade_date,
        s.ticker,
        s.sentiment_source,
        s.sentiment_score,
        p.fwd_return_5d,
        p.fwd_return_20d
    FROM sentiment s
    INNER JOIN prices p
        ON s.trade_date = p.trade_date
       AND s.ticker     = p.ticker
),

date_stats AS (
    SELECT
        COUNT(DISTINCT trade_date) AS n_days
    FROM joined
),

labeled AS (
    SELECT
        j.*,
        RANK() OVER (ORDER BY trade_date) AS day_rank,
        d.n_days
    FROM joined j
    CROSS JOIN date_stats d
)

SELECT
    trade_date,
    ticker,
    sentiment_source,
    sentiment_score,
    fwd_return_5d,
    fwd_return_20d,
    CASE
        WHEN day_rank <= n_days / 2 THEN 'EARLY'
        ELSE 'LATE'
    END AS period_label
FROM labeled
WHERE sentiment_score IS NOT NULL
  AND fwd_return_20d IS NOT NULL
