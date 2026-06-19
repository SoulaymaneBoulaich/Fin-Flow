-- daily_ohlcv.sql — Mart model: daily OHLCV summary per ticker
-- Materialization: incremental (only processes new days)

{{ config(
    materialized='incremental',
    schema='marts_finance',
    unique_key='ticker_date_key',
    incremental_strategy='merge'
) }}

WITH ticks AS (
    SELECT *
    FROM {{ ref('stg_stock_ticks') }}
    {% if is_incremental() %}
        -- Only process data not yet in the target table
        WHERE CAST(tick_ts AS DATE) > (
            SELECT COALESCE(MAX(trade_date), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
),

-- First and last tick per ticker/day (for open and close)
ordered AS (
    SELECT
        ticker,
        CAST(tick_ts AS DATE)   AS trade_date,
        open,
        close,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, CAST(tick_ts AS DATE)
            ORDER BY tick_ts ASC
        )                       AS rn_first,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, CAST(tick_ts AS DATE)
            ORDER BY tick_ts DESC
        )                       AS rn_last
    FROM ticks
),

first_ticks AS (
    SELECT ticker, trade_date, open AS day_open
    FROM ordered WHERE rn_first = 1
),

last_ticks AS (
    SELECT ticker, trade_date, close AS day_close
    FROM ordered WHERE rn_last = 1
),

daily_agg AS (
    SELECT
        t.ticker,
        CAST(t.tick_ts AS DATE)             AS trade_date,
        MAX(t.high)                         AS high,
        MIN(t.low)                          AS low,
        SUM(t.volume)                       AS total_volume,
        COUNT(*)                            AS tick_count,
        -- VWAP = Σ(close × volume) / Σvolume
        ROUND(
            SUM(t.close * t.volume) /
            NULLIF(SUM(t.volume), 0),
        4)                                  AS vwap,
        CURRENT_TIMESTAMP()                 AS computed_at
    FROM ticks t
    GROUP BY t.ticker, CAST(t.tick_ts AS DATE)
)

SELECT
    a.ticker,
    a.trade_date,
    f.day_open                              AS open,
    a.high,
    a.low,
    l.day_close                             AS close,
    a.total_volume                          AS volume,
    a.vwap,
    a.tick_count,
    a.computed_at,
    -- Surrogate key for merge
    MD5(CONCAT(a.ticker, '_', CAST(a.trade_date AS STRING))) AS ticker_date_key

FROM daily_agg a
LEFT JOIN first_ticks f ON a.ticker = f.ticker AND a.trade_date = f.trade_date
LEFT JOIN last_ticks  l ON a.ticker = l.ticker AND a.trade_date = l.trade_date
