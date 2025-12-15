{{ config( 
    materialized = 'table',
    schema = 'intermediate',
    alias = 'mag7_ta',
    partition_by = {
      "field": "trade_date",
      "data_type": "date"
    },
    cluster_by = ["ticker"],
    tags = ["intermediate", "ta", "mag7"]
) }}

-- 1. Get base prices from staging table
WITH prices AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume
  FROM {{ ref('stg_mag7') }}
),

-- 2. Add lags needed for returns & ATR calc
lagged AS (
  SELECT
    *,
    LAG(adj_close, 1)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_1,
    LAG(adj_close, 5)  OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_5,
    LAG(adj_close, 10) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_10,
    LAG(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_adj_close_20,
    LAG(close, 1)      OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_close
  FROM prices
),

-- 3. Compute point-in-time metrics: returns, true range (for ATR)
returns AS (
  SELECT
    *,
    -- 1-day, 5-day & 20-day returns
    SAFE_DIVIDE(adj_close - prev_adj_close_1, prev_adj_close_1) AS return_1d,
    SAFE_DIVIDE(adj_close - prev_adj_close_5,  prev_adj_close_5)  AS return_5d,
    SAFE_DIVIDE(adj_close - prev_adj_close_10, prev_adj_close_10) AS return_10d,
    SAFE_DIVIDE(adj_close - prev_adj_close_20, prev_adj_close_20) AS return_20d,

    -- True range for ATR
    GREATEST(
      high - low,
      ABS(high - prev_close),
      ABS(low  - prev_close)
    ) AS true_range
  FROM lagged
),

-- 4. Add *forward* prices for forward-return calculations
forward AS (
  SELECT
    *,
    LEAD(adj_close,  1) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_1,
    LEAD(adj_close,  5) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_5,
    LEAD(adj_close, 10) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_10,
    LEAD(adj_close, 20) OVER (PARTITION BY ticker ORDER BY trade_date) AS next_adj_close_20
  FROM returns
),

-- 5) Rolling features (compute first, then derive from them later)
rolling AS (
  SELECT
    trade_date,
    ticker,
    open,
    high,
    low,
    close,
    adj_close,
    volume,

    -- === Backward returns (historical) ===
    return_1d,
    return_5d,
    return_10d,
    return_20d,

    -- === Forward returns (for alpha / regime analysis) ===
    SAFE_DIVIDE(next_adj_close_1  - adj_close, adj_close) AS fwd_return_1d,
    SAFE_DIVIDE(next_adj_close_5  - adj_close, adj_close) AS fwd_return_5d,
    SAFE_DIVIDE(next_adj_close_10 - adj_close, adj_close) AS fwd_return_10d,
    SAFE_DIVIDE(next_adj_close_20 - adj_close, adj_close) AS fwd_return_20d,

    -- === Rolling volatility (stddev of daily returns) ===
    STDDEV_SAMP(return_1d) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS vola_20d,

    STDDEV_SAMP(return_1d) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) AS vola_60d,

    -- === Cumulative return over ~1 month (sum of daily returns over 20 days) ===
    SUM(return_1d) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS cumsum_return_20d,

    -- === Rolling price levels (20-day high/low) ===
    MAX(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS rolling_max_20d,

    MIN(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS rolling_min_20d,

    -- === Rolling price levels (200-day high/low) ===
    MAX(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS rolling_max_200d,

    MIN(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS rolling_min_200d,

    -- === Price z-score vs 20-day window ===
  SAFE_DIVIDE(
        adj_close - AVG(adj_close) OVER (
          PARTITION BY ticker ORDER BY trade_date
          ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        NULLIF(
          STDDEV_SAMP(adj_close) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
          ),
          0
        )
      ) AS price_zscore_20d,

    -- === ATR(14) ===
    AVG(true_range) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS atr_14,

    -- === Moving averages (MAs) ===
    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS ma_12,       -- for SMA-MACD calc

    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS ma_20,       -- ~1 trading month

    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 26 PRECEDING AND CURRENT ROW
    ) AS ma_26,       -- for SMA-MACD calc

    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS ma_50,       -- mid-term

    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) AS ma_100,       -- mid-long-term

    AVG(adj_close) OVER (
      PARTITION BY ticker
      ORDER BY trade_date
      ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS ma_200,       -- long-term

    -- Bollinger (20d): basis + std
    STDDEV_SAMP(adj_close) OVER (
      PARTITION BY ticker ORDER BY trade_date
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS bb_std_20d,

    -- RSI(14): compute avg gain/loss using return_1d
    AVG(GREATEST(return_1d, 0)) OVER (
      PARTITION BY ticker ORDER BY trade_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS rsi_avg_gain_14,

    AVG(GREATEST(-return_1d, 0)) OVER (
      PARTITION BY ticker ORDER BY trade_date
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS rsi_avg_loss_14

  FROM forward
  ),

-- 5.1) Add row no. to rolling CTE
rolling_ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date) AS rn
  FROM rolling r
),

vol_p80 AS (
  SELECT
    r.ticker,
    r.trade_date,
    APPROX_QUANTILES(x.vola_20d, 1000)[OFFSET(800)] AS vola_p80_252d
  FROM rolling_ranked r
  JOIN rolling_ranked x
    ON x.ticker = r.ticker
   AND x.rn BETWEEN r.rn - 251 AND r.rn
   AND x.vola_20d IS NOT NULL
  GROUP BY 1, 2
),

-- 6) Derived features that depend on rolling features and vol_p80

final AS (
  SELECT
    r.*,

    -- Bollinger bands (20d)
    r.ma_20 AS bb_mid_20d,
    (r.ma_20 + 2 * r.bb_std_20d) AS bb_upper_20d,
    (r.ma_20 - 2 * r.bb_std_20d) AS bb_lower_20d,

    -- RSI(14)
    100 - SAFE_DIVIDE(
      100,
      1 + SAFE_DIVIDE(r.rsi_avg_gain_14, NULLIF(r.rsi_avg_loss_14, 0))
    ) AS rsi_14,

    -- SMA-MACD proxy (useful for research; avoids EMA recursion complexity)
    (r.ma_12 - r.ma_26) AS macd_sma_12_26,
    AVG(r.ma_12 - r.ma_26) OVER (
      PARTITION BY ticker ORDER BY trade_date
      ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
    ) AS macd_signal_sma_9,

    -- Vol z-score of vola_20d vs trailing 252d (≈1 trading year)
    SAFE_DIVIDE(
      r.vola_20d - AVG(r.vola_20d) OVER (
        PARTITION BY ticker ORDER BY trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
      ),
      NULLIF(
        STDDEV_SAMP(r.vola_20d) OVER (
          PARTITION BY ticker ORDER BY trade_date
          ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ),
        0
      )
    ) AS vola_z20d,

    vp.vola_p80_252d,

    -- REV rule helper
    (r.vola_20d IS NOT NULL AND vp.vola_p80_252d IS NOT NULL AND r.vola_20d <= vp.vola_p80_252d)
      AS vola_not_top_20_252d

  FROM rolling_ranked r
  LEFT JOIN vol_p80 vp
    USING (ticker, trade_date)
)

SELECT * FROM final