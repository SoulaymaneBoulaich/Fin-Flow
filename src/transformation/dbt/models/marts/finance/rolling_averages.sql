-- rolling_averages.sql — Moving average mart (7, 14, 30-day SMA + EMA)

{{ config(materialized='table', schema='marts_finance') }}

WITH daily AS (
    SELECT *
    FROM {{ ref('daily_ohlcv') }}
),

-- Simple Moving Averages using window functions
moving_avgs AS (
    SELECT
        ticker,
        trade_date,
        close,
        volume,
        vwap,

        -- 7-day SMA
        ROUND(AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS sma_7,

        -- 14-day SMA
        ROUND(AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ), 4) AS sma_14,

        -- 30-day SMA
        ROUND(AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS sma_30,

        -- Row count within window (to detect insufficient data)
        COUNT(*) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS window_count,

        CURRENT_TIMESTAMP() AS computed_at

    FROM daily
)

SELECT
    ticker,
    trade_date,
    close,
    volume,
    vwap,
    sma_7,
    sma_14,
    -- Only return 30-day MA when we have >= 30 days of data
    CASE WHEN window_count >= 30 THEN sma_30 ELSE NULL END AS sma_30,
    -- SMA crossover signal: bullish when short-term > long-term
    CASE
        WHEN sma_7 > sma_30 THEN 'BULLISH'
        WHEN sma_7 < sma_30 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END AS ma_signal,
    computed_at

FROM moving_avgs
