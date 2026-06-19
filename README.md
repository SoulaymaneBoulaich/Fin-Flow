# FinFlow Enterprise Stock Market Data Engineering Platform

FinFlow is an end-to-end stock market data engineering platform built on the Apache ecosystem. It provides real-time event streaming, multi-tier data warehousing (Medallion Architecture), automated analytical transformations, role-based access control (RBAC), and GDPR-compliant privacy structures.

---

## Architectural Blueprint

The platform employs a Medallion Architecture (Bronze, Silver, Gold storage tiers) backed by high-throughput streaming and OLAP engines.

1. **Generation Layer**: yFinance wrapper threads publishing live market stock tickers, Faker-based synthetic user profiling, and Poisson trade event generators.
2. **Ingestion Layer**: A three-node Apache Kafka cluster receiving ticks and user events. Apache NiFi coordinates flow pipelines.
3. **Storage Layer**: MinIO (S3-compatible Object Lake) stores Parquet and JSON files. Apache Cassandra handles high-frequency time-series price data. PostgreSQL acts as a transactional metadata database.
4. **Transformation Layer**: Apache Spark streaming consumes Kafka logs directly into the Bronze tier. Spark batch jobs perform Silver tier deduplication/pseudonymization and Gold tier metric computation (Moving Averages, Volatility). dbt manages internal staging and mart views.
5. **Serving Layer**: FastAPI exposes REST endpoints for tickers, historical statistics, user portfolios, and service health checks.
6. **Operations Console**: A modern single-page dashboard illustrating real-time charts, service statuses, and privacy controls.
7. **Security, Privacy & Governance**: Apache Ranger handles role-based authorization, column-masking, and row-filtering. PII data is pseudonymized using HMAC-SHA256. GDPR scripts handle user data erasure and audit logs. Apache Atlas tracks metadata cataloging and lineage.
8. **Orchestration Layer**: Apache Airflow schedules daily transformations, hourly ingest cycles, and weekly GDPR sweeps.

---

## Directory Schema

```
finflow/
├── docker-compose.yml        # Orchestrates the 15+ containerized platform services
├── .env                      # Unified environment variables for local/production runs
├── pyproject.toml            # Pytest configuration and project metadata
├── conftest.py               # Shared testing fixtures and in-memory mock databases
├── src/                      # Core platform source code
│   ├── generation/           # Real stock producers and simulators
│   ├── ingestion/            # Kafka producers, consumers, and NiFi control API
│   ├── storage/              # MinIO, Cassandra, and PostgreSQL client wrappers
│   ├── transformation/       # Spark transformations and dbt analytical models
│   ├── serving/              # FastAPI REST endpoints and the static dashboard
│   ├── security/             # Apache Ranger RBAC integration
│   ├── privacy/              # PII engines and GDPR erasure utilities
│   └── governance/           # Apache Atlas lineage and asset registration
└── tests/                    # Integration and unit test suite
```

---

## Enterprise Deployment & Scaling Strategies

To project and scale this architecture in an enterprise environment:

### High Availability Storage
- **Object Storage**: Deploy MinIO in a distributed Multi-Tenant tenant pool across multiple nodes with erasure coding enabled to handle drive failures.
- **Time-Series database**: Scale the Apache Cassandra cluster across multiple data centers with a replication factor of 3, using Time Window Compaction Strategy (TWCS) to optimize TTLs for tick files.
- **Relational Metadata**: Deploy PostgreSQL with primary-replica replication and connection pooling (e.g., PgBouncer) to handle metadata tasks.

### Stream Processing & Kubernetes Orchestration
- **Containerization**: Deploy the stack using Kubernetes (EKS, GKE, or AKS) using official Helm Charts for Kafka, Airflow, and Spark.
- **Spark on K8s**: Configure Spark to run natively on Kubernetes, spinning up dynamic executor pods that scale based on queue size and release resources upon task completion.
- **Kafka Tuning**: Configure Kafka with 3 replicas, partition sizes matching CPU core multiples, and producer settings (`enable.idempotence=true`, `acks=all`) to guarantee exactly-once processing.

### Enterprise Governance & RBAC
- **Apache Atlas Integration**: Hook the Hive Metastore and Spark listeners directly into Atlas to automatically document dataset structures and trace structural data lineage on every run.
- **Ranger Security Policy**: Centralize permissions at the gateway level. Use Ranger to mask user emails and names, filter rows based on location tags, and audit access across S3 resources.

---

## Operating instructions

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Virtual Environment (.venv)

### Startup Sequence

1. Clone the repository and navigate to the directory:
   ```bash
   git clone https://github.com/SoulaymaneBoulaich/Fin-Flow.git
   cd Fin-Flow
   ```

2. Spin up the infrastructure containers:
   ```bash
   docker compose up -d
   ```

3. Initialize the Python virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

4. Set environment overrides to target localhost services and start the FastAPI dashboard:
   ```bash
   export POSTGRES_HOST="localhost"
   export MINIO_ENDPOINT="http://localhost:9000"
   export KAFKA_BROKERS="localhost:9092,localhost:9093,localhost:9094"
   
   uvicorn src.serving.api.main:app --host 0.0.0.0 --port 8000
   ```

5. Access the operational console in your browser:
   ```
   http://localhost:8000
   ```

### Executing the Test Suite

Run the unit and integration tests to verify the integrity of the database, API, and PII modules:
```bash
pytest tests/ -v --tb=short
```
