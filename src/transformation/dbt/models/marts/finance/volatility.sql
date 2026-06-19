-- volatility.sql — Rolling annualized volatility per ticker

{{ config(materialized='table', schema='marts_finance') }}

WITH daily AS (
    SELECT * FROM {{ ref('daily_ohlcv') }}
),

log_returns AS (
    SELECT
        ticker,
        trade_date,
        close,
        -- Daily log return: ln(close_t / close_{t-1})
        LN(close / LAG(close) OVER (
            PARTITION BY ticker ORDER BY trade_date
        )) AS log_return
    FROM daily
),

volatility AS (
    SELECT
        ticker,
        trade_date,
        close,
        log_return,

        -- 30-day annualized volatility = std(log_returns) × √252
        ROUND(
            STDDEV(log_return) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) * SQRT(252),
        6) AS volatility_30d_annualized,

        -- 90-day annualized volatility
        ROUND(
            STDDEV(log_return) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
            ) * SQRT(252),
        6) AS volatility_90d_annualized,

        -- Volatility regime classification
        CASE
            WHEN STDDEV(log_return) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) * SQRT(252) < 0.15 THEN 'LOW'
            WHEN STDDEV(log_return) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) * SQRT(252) < 0.35 THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS volatility_regime,

        COUNT(*) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS days_in_window,

        CURRENT_TIMESTAMP() AS computed_at

    FROM log_returns
)

SELECT *
FROM volatility
WHERE log_return IS NOT NULL
