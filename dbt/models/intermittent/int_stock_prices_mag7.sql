{{ config(
    materialized = 'table',
    schema = 'intermittent',
    alias = 'stock_prices_mag7_ta',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by = ["ticker"],
    tags = ["intermediate", "ta", "mag7"]
) }}

WITH prices AS (
  SELECT
    date        AS trade_date,
    ticker,
    close,
    adj_close
  FROM {{ source('staging', 'stock_prices_mag7') }}
)

SELECT
  trade_date,
  ticker,
  close,
  adj_close,

  -- Short / mid / long term moving averages (tune windows)
  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
  ) AS ma_20,       -- ~1 trading month

  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
  ) AS ma_50,       -- mid-term

  AVG(adj_close) OVER (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
  ) AS ma_200,      -- long-term

  -- Daily return
  (adj_close /
   LAG(adj_close) OVER (PARTITION BY ticker ORDER BY trade_date) - 1
  ) AS daily_return

FROM prices;
