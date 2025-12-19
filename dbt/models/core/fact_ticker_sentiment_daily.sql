-- models/core/fact_ticker_sentiment_daily.sql
{{ config(
    materialized         = 'incremental',
    incremental_strategy = 'insert_overwrite',
    schema       = 'core',
    alias        = 'fact_ticker_sentiment_daily',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by   = ['ticker'],
    tags         = ['core', 'fact', 'ticker', 'sentiment']
) }}

WITH n AS (
  SELECT *
  FROM {{ ref('int_sentiment_ticker_daily') }}
  {% if is_incremental() %}
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
),
g AS (
  SELECT *
  FROM {{ ref('int_gkg_ticker_daily') }}
  {% if is_incremental() %}
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
),

combined AS (
  SELECT
      COALESCE(n.trade_date, g.trade_date) AS trade_date,
      COALESCE(n.ticker,     g.ticker)     AS ticker,

      -- News (FinBERT) daily stats
      n.article_count,
      n.sentiment_mean,
      n.sentiment_median,
      n.sentiment_stddev,
      n.pos_count,
      n.neg_count,
      n.neu_count,
      n.sentiment_balance,

      -- GDELT daily stats
      g.event_count,
      g.tone_mean,
      g.tone_stddev
  FROM n
  FULL OUTER JOIN g
    ON n.trade_date = g.trade_date
   AND n.ticker     = g.ticker
)

SELECT * FROM combined
