{{ config(
    materialized = 'incremental',
    schema       = 'intermediate',
    alias        = 'sentiment_ticker_daily',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ['ticker'],
    incremental_strategy = 'insert_overwrite',
    tags         = ['intermediate', 'news', 'sentiment', 'ticker']
) }}

WITH base AS (
    SELECT
        CAST(news_ts AS DATE) AS trade_date,
        ticker,
        sentiment_score,
        sentiment_label
    FROM {{ ref('stg_news_headlines') }}
    WHERE ticker IS NOT NULL
    {% if is_incremental() %}
      AND CAST(news_ts AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

agg AS (
    SELECT
        trade_date,
        ticker,
        COUNT(*) AS article_count,
        AVG(sentiment_score) AS sentiment_mean,
        STDDEV_SAMP(sentiment_score) AS sentiment_stddev,
        APPROX_QUANTILES(sentiment_score, 100)[OFFSET(50)] AS sentiment_median,

        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS pos_count,
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS neg_count,
        SUM(CASE WHEN sentiment_label = 'neutral'  THEN 1 ELSE 0 END) AS neu_count,

        SAFE_DIVIDE(
            SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) -
            SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END),
            COUNT(*)
        ) AS sentiment_balance
    FROM base
    GROUP BY trade_date, ticker
)

SELECT *
FROM agg
