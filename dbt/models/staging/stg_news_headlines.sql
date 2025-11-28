{{ config(
    materialized = 'incremental',
    schema = 'staging',
    alias = 'news_headlines',
    incremental_strategy = 'merge',
    unique_key = ['ticker', 'news_ts'],
    partition_by = { 'field': 'news_ts', 'data_type': 'timestamp' },
    cluster_by = ['ticker']
) }}

-- 1) Pull from raw, only new or later-fetched rows when incremental
WITH source_raw AS (
  SELECT *
  FROM {{ source('raw', 'news_headlines') }}
  {% if is_incremental() %}
    WHERE TIMESTAMP(fetched_at) >
      (SELECT COALESCE(MAX(fetched_at), TIMESTAMP('2000-01-01'))
       FROM {{ this }})
  {% endif %}
),

-- 2) Type casting
typed AS (
  SELECT
    ticker,
    headline,
    url,
    source,
  --  `window`,
    TIMESTAMP(timestamp)       AS news_ts,
    TIMESTAMP(fetched_at)      AS fetched_at,
    sentiment_label,
    SAFE_CAST(sentiment_score AS FLOAT64) AS sentiment_score
  FROM source_raw
),

-- 3) Dedup within this batch: keep latest fetched per (ticker, news_ts)
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ticker, news_ts
      ORDER BY fetched_at DESC
    ) AS rn
  FROM typed
)

SELECT
  ticker,
  headline,
  news_ts,
  url,
  source,
--  window,
  fetched_at,
  sentiment_label,
  sentiment_score
FROM deduped
WHERE rn = 1
