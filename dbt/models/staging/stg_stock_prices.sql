{{ config(
    materialized = 'incremental',
    schema = 'staging',
    alias = 'stock_prices',
    incremental_strategy = 'merge',
    unique_key = ['ticker', 'trade_date'],
    partition_by = { 'field': 'trade_date', 'data_type': 'date' },
    cluster_by = ['ticker']
) }}

-- 1) Pull from raw, only new rows when incremental
WITH source_incremental AS (
  SELECT *
  FROM {{ source('raw', 'stock_prices') }}
  {% if is_incremental() %}
    -- only rows fetched after the current max
    WHERE TIMESTAMP(fetched_at) >
      (SELECT COALESCE(MAX(fetched_at), TIMESTAMP('2000-01-01'))
       FROM {{ this }})
  {% endif %}
),

-- 2) Type casting
typed AS (
  SELECT
    ticker,
    PARSE_DATE('%Y-%m-%d', date)            AS trade_date,
    SAFE_CAST(open       AS FLOAT64)        AS open,
    SAFE_CAST(high       AS FLOAT64)        AS high,
    SAFE_CAST(low        AS FLOAT64)        AS low,
    SAFE_CAST(close      AS FLOAT64)        AS close,
    SAFE_CAST(adj_close  AS FLOAT64)        AS adj_close,
    SAFE_CAST(volume     AS INT64)          AS volume,
    TIMESTAMP(fetched_at)                   AS fetched_at
  FROM source_incremental
),

-- 3) Dedup within this batch: keep latest fetched per (ticker, trade_date)
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ticker, trade_date
      ORDER BY fetched_at DESC
    ) AS rn
  FROM typed
)

SELECT
  ticker,
  trade_date,
  open,
  high,
  low,
  close,
  adj_close,
  volume,
  fetched_at
FROM deduped
WHERE rn = 1
