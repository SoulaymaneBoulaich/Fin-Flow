-- stg_stock_ticks.sql — Staging model: clean raw ticks from Silver zone
-- Materialization: view (refreshed on every query)

{{ config(materialized='view', schema='staging') }}

SELECT
    event_id,
    UPPER(TRIM(ticker))                     AS ticker,
    CAST(timestamp AS TIMESTAMP)            AS tick_ts,
    CAST(open   AS DOUBLE)                  AS open,
    CAST(high   AS DOUBLE)                  AS high,
    CAST(low    AS DOUBLE)                  AS low,
    CAST(close  AS DOUBLE)                  AS close,
    CAST(volume AS BIGINT)                  AS volume,
    COALESCE(CAST(vwap AS DOUBLE), close)   AS vwap,
    source,
    _dedup_hash,
    CAST(year  AS INT)                      AS year,
    CAST(month AS INT)                      AS month,
    CAST(day   AS INT)                      AS day

FROM {{ source('silver', 'stock_ticks') }}

WHERE
    close > 0
    AND volume >= 0
    AND timestamp IS NOT NULL
    AND ticker IS NOT NULL
