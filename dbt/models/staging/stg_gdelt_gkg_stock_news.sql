{{ config(
    materialized = 'incremental',
    schema = 'staging',
    alias = 'news_gkg',
    incremental_strategy = 'merge',
    unique_key = 'gkg_record_id',
    partition_by = {
      "field": "event_date",
      "data_type": "date"
    },
    cluster_by = ["ticker"],
    tags = ["staging", "gdelt", "mag7"]
) }}

-- 1) Pull from BQ pub data
WITH gkg AS (
  SELECT
    GKGRECORDID AS gkg_record_id,
    PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(DATE AS STRING)) AS event_ts,
    DATE(PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(DATE AS STRING))) AS event_date,
    DocumentIdentifier AS url,
    V2Organizations AS organizations_raw,
    V2Themes AS themes_raw,
    V2Tone AS tone_raw
  FROM {{ source('gdeltv2', 'gkg_partitioned') }}
  WHERE DATE IS NOT NULL

  -- ✅ Only scan recent partitions
  AND DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)

  {% if is_incremental() %}
    -- On later runs, only pick up *newer* partitions than what we already have
    AND DATE(_PARTITIONTIME) > (
      SELECT IFNULL(MAX(event_date), DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY))
      FROM {{ this }}
    )
  {% endif %}
),

-- Clean organization names into semicolon-separated list
orgs AS (
  SELECT
    *,
    SPLIT(organizations_raw, ';') AS org_list
  FROM gkg
),

-- Flatten organizations: one row per mentioned org
flattened AS (
  SELECT
    gkg_record_id,
    event_ts,
    event_date,
    url,
    org AS organization,
    themes_raw,
    tone_raw
  FROM orgs, UNNEST(org_list) AS org
  WHERE org IS NOT NULL AND LENGTH(org) > 0
),

-- Join to stock tickers dimension
matched AS (
  SELECT
    f.*,
    d.ticker,
    d.company_name
  FROM flattened f
  JOIN `mag7_intel_dims.dim_ticker` d
    ON REGEXP_CONTAINS(UPPER(f.organization), UPPER(d.company_name))
)

SELECT
  gkg_record_id,
  event_ts,
  event_date,
  url,
  organization AS matched_org_name,
  ticker,
  company_name,
  tone_raw AS tone,
  themes_raw AS themes
FROM matched
