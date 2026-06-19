"""
dag_ingest_batch.py — Hourly batch ingestion DAG.

Schedule: Every hour.
Tasks:
 1. trigger_nifi_batch     — Call NiFi API to start batch ingest flow
 2. wait_bronze_landing    — Wait until new Bronze files appear in MinIO
 3. spark_bronze_to_silver — Run Bronze→Silver Spark transformation
 4. run_data_quality       — Check Silver data quality
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    "owner": "finflow-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_APP_PATH = "/opt/airflow/src/transformation/spark"


def _trigger_nifi(**ctx):
    """Trigger NiFi batch ingestion flow via REST API."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")
    from ingestion.nifi.nifi_api import NiFiAPIClient

    client = NiFiAPIClient(host="nifi", port=8443)
    if not client.is_healthy():
        # NiFi may not be deployed — log and continue
        return "NiFi not available — batch ingest skipped"

    groups = client.list_process_groups("root")
    batch_groups = [g for g in groups if "batch" in g.get("component", {}).get("name", "").lower()]

    if batch_groups:
        group_id = batch_groups[0]["component"]["id"]
        client.trigger_batch_ingest(group_id, wait_seconds=10)
        return f"NiFi batch triggered: group_id={group_id}"
    return "No batch process group found in NiFi"


def _wait_for_bronze(**ctx):
    """Poll MinIO Bronze zone until new files appear (with timeout)."""
    import time
    from minio import Minio

    client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", "finflow_admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "FinFlow_Secret_2024!"),
        secure=False,
    )

    max_wait = 300  # 5 minutes
    poll_interval = 15
    elapsed = 0

    while elapsed < max_wait:
        objects = list(client.list_objects("finflow", prefix="bronze/", recursive=True))
        if objects:
            return f"Bronze data found: {len(objects)} files"
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError("Bronze zone still empty after 5 minutes")


def _run_data_quality(**ctx):
    """Run basic data quality checks on Silver data and log results."""
    import sys
    sys.path.insert(0, "/opt/airflow/src")

    checks_passed = []
    checks_failed = []

    try:
        from minio import Minio
        client = Minio(
            "minio:9000",
            access_key=os.getenv("MINIO_ACCESS_KEY", "finflow_admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "FinFlow_Secret_2024!"),
            secure=False,
        )
        silver_objects = list(client.list_objects("finflow", prefix="silver/", recursive=True))
        if silver_objects:
            checks_passed.append(f"Silver non-empty: {len(silver_objects)} files")
        else:
            checks_failed.append("Silver zone is EMPTY after transformation")
    except Exception as exc:
        checks_failed.append(f"MinIO check failed: {exc}")

    if checks_failed:
        raise ValueError(f"Data quality failures: {checks_failed}")

    return {"passed": checks_passed, "failed": []}


with DAG(
    dag_id="dag_ingest_batch",
    description="Hourly batch ingestion: NiFi trigger → Bronze → Silver",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["finflow", "ingestion", "hourly"],
) as dag:

    trigger_nifi = PythonOperator(
        task_id="trigger_nifi_batch",
        python_callable=_trigger_nifi,
    )

    wait_bronze = PythonOperator(
        task_id="wait_bronze_landing",
        python_callable=_wait_for_bronze,
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"{SPARK_APP_PATH}/bronze_to_silver.py"
        ),
    )

    data_quality = PythonOperator(
        task_id="run_data_quality",
        python_callable=_run_data_quality,
    )

    trigger_nifi >> wait_bronze >> spark_bronze_to_silver >> data_quality
