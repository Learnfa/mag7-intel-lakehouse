{{ config(
  materialized         = 'incremental',
  schema               = 'intermediate',
  alias                = 'gkg_ticker_daily',
  partition_by = {
    "field": "trade_date",
    "data_type": "date"
  },
  cluster_by           = ['ticker'],
  incremental_strategy = 'insert_overwrite',
  tags                 = ['intermediate', 'gdelt', 'gkg', 'mag7']
) }}


WITH base AS (
    SELECT
        event_date AS trade_date,
        ticker,
        tone
    FROM {{ ref('stg_gdelt_gkg') }}
    WHERE ticker IS NOT NULL
    {% if is_incremental() %}
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    {% endif %}
),

agg AS (
    SELECT
        trade_date,
        ticker,
        COUNT(*) AS event_count,
        AVG(tone) AS tone_mean,
        STDDEV_SAMP(tone) AS tone_stddev
    FROM base
    GROUP BY trade_date, ticker
)

SELECT *
FROM agg
