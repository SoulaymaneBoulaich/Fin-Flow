#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# start.sh — Start the FinFlow platform
# ─────────────────────────────────────────────────────────────────────────────
set -e

echo "╔══════════════════════════════════════════════════════════╗"
echo "║           FinFlow Data Engineering Platform              ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Load environment variables
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Create necessary local directories
mkdir -p logs/airflow
mkdir -p src/orchestration/dags

echo "[1/4] Starting infrastructure services..."
docker compose up -d postgres zookeeper minio cassandra
sleep 10

echo "[2/4] Starting Kafka cluster..."
docker compose up -d kafka-1 kafka-2 kafka-3
sleep 20

echo "[3/4] Initializing Kafka topics and MinIO buckets..."
docker compose up kafka-init minio-init
sleep 5

echo "[4/4] Starting all platform services..."
docker compose up -d

echo ""
echo "✅ FinFlow platform started! Services available at:"
echo "   • Kafka UI:     http://localhost:8090"
echo "   • MinIO:        http://localhost:9001  (admin / FinFlow_Secret_2024!)"
echo "   • Airflow:      http://localhost:8080  (admin / admin123)"
echo "   • Spark UI:     http://localhost:8081"
echo "   • Druid:        http://localhost:8888"
echo "   • Superset:     http://localhost:8088  (admin / admin123)"
echo "   • FastAPI Docs: http://localhost:8000/docs"
echo ""
echo "Run './scripts/test_e2e.sh' to validate the full pipeline."
