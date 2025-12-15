{{ config(
    materialized = 'table',
    schema       = 'mart',
    alias        = 's1_core_momrev',
    partition_by = { "field": "trade_date", "data_type": "date" },
    cluster_by   = ["ticker"],
    tags         = ['mart', 'signal', 'core', 'momrev']
) }}

-- ---------------------------------------------------------------------
-- s1_signal_core_momrev
--
-- Canonical MOM / REV / NEU signal labels for Streamlit shading + evidence.
--
-- MOM if:
--   adj_close > ma_100
--   AND regime_bucket_10 BETWEEN 8 AND 10
--   AND vola_z20d < 1
--
-- REV if:
--   regime_bucket_10 <= 3
--   AND price_zscore_20d <= -1
--   AND vola_not_top_20_252d = TRUE
--
-- Else: NEU
-- ---------------------------------------------------------------------

WITH joined AS (
  SELECT
    r.trade_date,
    r.ticker,

    -- price (from intermediate TA for consistency)
    ta.adj_close,

    -- regimes (core)
    r.regime_bucket_10,
    r.price_zscore_20d,

    -- forward returns for evidence charts
    r.fwd_return_5d,
    r.fwd_return_10d,
    r.fwd_return_20d,

    -- TA / risk (intermediate)
    ta.ma_100,
    ta.vola_z20d,
    ta.vola_not_top_20_252d,

    CURRENT_TIMESTAMP() AS asof_ts,
    'S1_momrev_v1' AS signal_version

  FROM {{ ref('fact_regimes') }} r
  LEFT JOIN {{ ref('int_mag7_ta') }} ta
    USING (trade_date, ticker)
),

labeled AS (
  SELECT
    *,

    -- Data completeness flags
    (adj_close IS NOT NULL AND ma_100 IS NOT NULL AND regime_bucket_10 IS NOT NULL) AS has_mom_inputs,
    (regime_bucket_10 IS NOT NULL AND price_zscore_20d IS NOT NULL AND vola_not_top_20_252d IS NOT NULL) AS has_rev_inputs,

    -- MOM signal
    CASE
      WHEN adj_close > ma_100
       AND regime_bucket_10 BETWEEN 8 AND 10
       AND vola_z20d < 1
      THEN TRUE ELSE FALSE
    END AS is_mom,

    -- REV signal
    CASE
      WHEN regime_bucket_10 <= 3
       AND price_zscore_20d <= -1
       AND vola_not_top_20_252d = TRUE
      THEN TRUE ELSE FALSE
    END AS is_rev

  FROM joined
),

final AS (
  SELECT
    *,
    -- Resolve state: MOM > REV > NEU (no overlap expected, but define precedence)
    CASE
      WHEN is_mom THEN 'MOM'
      WHEN is_rev THEN 'REV'
      ELSE 'NEU'
    END AS signal_state,

    -- Human-readable reason (useful for debugging)
    CASE
      WHEN adj_close IS NULL THEN 'missing_price'
      WHEN ma_100 IS NULL THEN 'missing_ma100'
      WHEN vola_z20d IS NULL THEN 'missing_vola_z20d'
      WHEN vola_not_top_20_252d IS NULL THEN 'missing_vola_gate'
      WHEN regime_bucket_10 IS NULL THEN 'missing_regime_bucket'
      WHEN price_zscore_20d IS NULL THEN 'missing_price_zscore'
      WHEN is_mom THEN 'mom_conditions_met'
      WHEN is_rev THEN 'rev_conditions_met'
      ELSE 'neutral_conditions'
    END AS signal_reason

  FROM labeled
)

SELECT
  trade_date,
  ticker,

  -- price
  adj_close,

  -- TA / risk
  ma_100,
  vola_z20d,
  vola_not_top_20_252d,

  -- regimes
  regime_bucket_10,
  price_zscore_20d,

  -- evidence
  fwd_return_5d,
  fwd_return_10d,
  fwd_return_20d,

  -- outputs
  is_mom,
  is_rev,
  signal_state,
  signal_reason,

  -- metadata
  asof_ts,
  signal_version

FROM final
