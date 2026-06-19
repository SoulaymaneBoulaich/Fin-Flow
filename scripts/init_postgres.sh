#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# init_postgres.sh — Create multiple databases on first startup
# ─────────────────────────────────────────────────────────────────────────────
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE hive_metastore;
    CREATE DATABASE finflow_app;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE finflow_app TO $POSTGRES_USER;
EOSQL

echo "All databases created successfully."
