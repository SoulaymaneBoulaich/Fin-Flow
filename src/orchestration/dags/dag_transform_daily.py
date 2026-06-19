"""
dag_transform_daily.py — Main daily ETL DAG.

Schedule: Every day at midnight UTC.
Tasks:
 1. sensor_silver_ready    — Wait for Silver data to exist
 2. spark_silver_to_gold   — Run Silver→Gold Spark job
 3. dbt_run                — Execute all dbt mart models
 4. dbt_test               — Run dbt data quality tests
 5. load_to_druid          — Submit Druid ingestion spec
 6. notify_success         — Send success notification
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago


# ─── Default args ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "finflow-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_APP_PATH = "/opt/airflow/src/transformation/spark"
DBT_PATH = "/opt/airflow/src/transformation/dbt"
DRUID_COORDINATOR = os.getenv("DRUID_COORDINATOR", "druid-coordinator")


def _check_silver_data(**ctx):
    """Python sensor: verify Silver zone has data for today's run."""
    from minio import Minio
    client = Minio(
        "minio:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", "finflow_admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "FinFlow_Secret_2024!"),
        secure=False,
    )
    objects = list(client.list_objects("finflow", prefix="silver/stock-ticks/", recursive=False))
    if not objects:
        raise ValueError("Silver zone is empty — skipping daily transform")
    return f"Silver data found: {len(objects)} partitions"


def _submit_druid_ingestion(**ctx):
    """Submit stock-ticks Kafka ingestion spec to Druid coordinator."""
    import json
    import requests

    spec_path = "/opt/airflow/src/../config/druid/ingestion-specs/stock_ticks_spec.json"
    with open(spec_path) as f:
        spec = json.load(f)

    resp = requests.post(
        f"http://{DRUID_COORDINATOR}:8081/druid/indexer/v1/supervisor",
        json=spec,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Druid supervisor submission failed: {resp.status_code} {resp.text}")

    return f"Druid supervisor submitted: {resp.json()}"


def _notify_success(**ctx):
    """Log success notification (Slack webhook can be plugged in here)."""
    dag_run = ctx["dag_run"]
    import os
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        import requests
        requests.post(webhook_url, json={
            "text": f"✅ FinFlow daily transform complete! "
                    f"Execution date: {dag_run.execution_date}"
        })
    return "Notification sent"


def _notify_failure(context):
    """Callback for task failure notification."""
    task = context.get("task_instance")
    import os
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        import requests
        requests.post(webhook_url, json={
            "text": f"❌ FinFlow DAG FAILED! Task: {task.task_id}"
        })


# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_transform_daily",
    description="Daily ETL: Silver→Gold via Spark, dbt models, Druid load",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",  # Midnight UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["finflow", "etl", "daily"],
) as dag:

    # ─── Task 1: Check Silver has data ────────────────────────────────────────
    check_silver = PythonOperator(
        task_id="check_silver_data",
        python_callable=_check_silver_data,
        on_failure_callback=_notify_failure,
    )

    # ─── Task 2: Run Spark Silver→Gold job ───────────────────────────────────
    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=(
            f"spark-submit "
            f"--master {SPARK_MASTER} "
            f"--deploy-mode client "
            f"--conf spark.sql.shuffle.partitions=20 "
            f"{SPARK_APP_PATH}/silver_to_gold.py"
        ),
        on_failure_callback=_notify_failure,
    )

    # ─── Task 3: Run dbt models ───────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PATH} && dbt run --profiles-dir /opt/airflow/config",
        on_failure_callback=_notify_failure,
    )

    # ─── Task 4: Run dbt tests ────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PATH} && dbt test --profiles-dir /opt/airflow/config",
        on_failure_callback=_notify_failure,
    )

    # ─── Task 5: Submit Druid ingestion ───────────────────────────────────────
    load_to_druid = PythonOperator(
        task_id="load_to_druid",
        python_callable=_submit_druid_ingestion,
        on_failure_callback=_notify_failure,
    )

    # ─── Task 6: Notify success ───────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
    )

    # ─── Dependencies ─────────────────────────────────────────────────────────
    check_silver >> spark_silver_to_gold >> dbt_run >> dbt_test >> load_to_druid >> notify
