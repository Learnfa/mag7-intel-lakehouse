{{ config(
    materialized         = 'table',
    schema               = 'mart',
    alias                = 'overview_today',
    cluster_by           = ['ticker'],
    tags                 = ['mart', 'overview', 'dashboard', 'today']
) }}

-- ---------------------------------------------------------------------
-- mart_overview_today
--
-- PURPOSE:
--   Single, thin "today snapshot" table for the Overview page.
--
-- GRAIN:
--   trade_date × ticker  (MAG7 equities only)
--
-- INCLUDES:
--   - Price + TA (from fact_price_features)
--   - Sentiment + news volume (from market_sentiment_ts)
--   - Core signal state + score (from s0_core_value)
--   - Macro context (Fear & Greed + regime)
--
-- NOTES:
--   - No forward returns
--   - No look-ahead bias
--   - Designed for fast Streamlit loading
-- ---------------------------------------------------------------------

-- 0) As-of date anchor (align everything to the latest signal date)
WITH asof AS (
    SELECT
        MAX(trade_date) AS asof_date
    FROM {{ ref('mart_s0_core_value') }}
),

-- 1) Core signal snapshot (authoritative signal state)
core AS (
    SELECT
        trade_date,
        ticker,
        core_signal_state,
        core_score,
        core_score_norm,
        core_reason,
        regime_bucket_10,
        zscore_bucket_10
    FROM {{ ref('mart_s0_core_value') }}
    WHERE trade_date = (SELECT asof_date FROM asof)
),

-- 2) Price + TA snapshot (full-featured, equity rows only)
price AS (
    SELECT
        trade_date,
        ticker,
        adj_close,
        volume,

        -- returns
        return_1d,
        return_5d,
        return_10d,
        return_20d,

        -- volatility / TA
        vola_20d,
        vola_60d,
        price_zscore_20d,
        atr_14,
        rsi_14,
        ma_20,
        ma_50,
        ma_200,

        -- benchmark-relative
        ndx_price_ratio,
        ndxe_price_ratio

    FROM {{ ref('fact_price_features') }}
    WHERE trade_date = (SELECT asof_date FROM asof)
      AND NOT STARTS_WITH(ticker, '^')   -- equities only
),

-- 3) Sentiment + macro snapshot (already aligned to trading dates)
sentiment AS (
    SELECT
        trade_date,
        ticker,

        -- sentiment volume & polarity
        article_count,
        sentiment_mean,
        finbert_net_rate,
        finbert_net_ma7,

        event_count,
        tone_mean,
        gdelt_tone_ma7,

        -- macro
        fear_greed,
        macro_regime_4,
        macro_risk_off_score_20d

    FROM {{ ref('mart_market_sentiment_ts') }}
    WHERE trade_date = (SELECT asof_date FROM asof)
),

-- 4) Combine everything
final AS (
    SELECT
        c.trade_date,
        c.ticker,

        -- ===== Price =====
        p.adj_close,
        p.volume,

        p.return_1d,
        p.return_5d,
        p.return_10d,
        p.return_20d,

        p.vola_20d,
        p.vola_60d,
        p.atr_14,
        p.rsi_14,

        p.ma_20,
        p.ma_50,
        p.ma_200,

        p.ndx_price_ratio,
        p.ndxe_price_ratio,

        -- ===== Sentiment =====
        s.article_count,
        s.sentiment_mean,
        s.finbert_net_rate,
        s.finbert_net_ma7,
        s.event_count,
        s.tone_mean,
        s.gdelt_tone_ma7,

        -- ===== Core Signal =====
        c.core_signal_state,
        c.core_score,
        c.core_score_norm,
        c.core_reason,
        c.regime_bucket_10,
        c.zscore_bucket_10,

        -- ===== Macro Context =====
        s.fear_greed,
        s.macro_regime_4,
        s.macro_risk_off_score_20d

    FROM core c
    LEFT JOIN price p
      ON c.trade_date = p.trade_date
     AND c.ticker     = p.ticker
    LEFT JOIN sentiment s
      ON c.trade_date = s.trade_date
     AND c.ticker     = s.ticker
)

SELECT *
FROM final
