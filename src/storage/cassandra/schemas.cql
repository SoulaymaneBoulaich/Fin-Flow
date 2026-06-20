-- ─────────────────────────────────────────────────────────────────────────────
-- Cassandra CQL schema definitions for FinFlow.
-- Run this file against Cassandra using: cqlsh -f schemas.cql
-- 
-- Tables:
--  - tick_data: High-frequency tick storage optimized for time-series access
--  - trade_events: Stored trade executions
--  - user_profiles: Denormalized user data for fast lookups
-- ─────────────────────────────────────────────────────────────────────────────

-- ─────────────────────────────────────────────────────────────────────────────
-- Create keyspace
-- ─────────────────────────────────────────────────────────────────────────────
CREATE KEYSPACE IF NOT EXISTS finflow
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND DURABLE_WRITES = true;

USE finflow;

-- ─────────────────────────────────────────────────────────────────────────────
-- TICK DATA TABLE
-- Partition key: ticker — keeps all ticks for one ticker on the same node
-- Clustering key: timestamp DESC — newest ticks first (most common query)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tick_data (
    ticker         TEXT,
    timestamp      TIMESTAMP,
    event_id       TEXT,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    volume         BIGINT,
    vwap           DOUBLE,
    source         TEXT,
    created_at     TIMESTAMP,
    PRIMARY KEY (ticker, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_size': '1',
      'compaction_window_unit': 'HOURS'
  }
  AND gc_grace_seconds = 86400
  AND default_time_to_live = 604800;  -- 7 days TTL

-- Index on event_id for deduplication checks
CREATE INDEX IF NOT EXISTS idx_tick_event_id ON tick_data (event_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- TRADE EVENTS TABLE
-- Partition key: ticker — co-locate trades for the same ticker
-- Clustering: timestamp DESC, trade_id for uniqueness within timestamp
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_events (
    ticker         TEXT,
    timestamp      TIMESTAMP,
    trade_id       TEXT,
    user_id        TEXT,
    action         TEXT,
    quantity       INT,
    price          DOUBLE,
    total_value    DOUBLE,
    status         TEXT,
    created_at     TIMESTAMP,
    PRIMARY KEY (ticker, timestamp, trade_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, trade_id ASC)
  AND default_time_to_live = 2592000;  -- 30 days TTL

-- ─────────────────────────────────────────────────────────────────────────────
-- USER PORTFOLIO TABLE
-- Partition key: user_id — all portfolio rows for one user on same node
-- Clustering: ticker for easy per-user ticker lookup
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_portfolio (
    user_id        TEXT,
    ticker         TEXT,
    shares_held    DOUBLE,
    avg_cost       DOUBLE,
    last_updated   TIMESTAMP,
    PRIMARY KEY (user_id, ticker)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- PIPELINE METRICS TABLE
-- Stores pipeline health metrics: row counts, latency, error rates
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    pipeline_name  TEXT,
    run_date       DATE,
    run_ts         TIMESTAMP,
    metric_name    TEXT,
    metric_value   DOUBLE,
    PRIMARY KEY (pipeline_name, run_date, run_ts, metric_name)
) WITH CLUSTERING ORDER BY (run_date DESC, run_ts DESC, metric_name ASC);
