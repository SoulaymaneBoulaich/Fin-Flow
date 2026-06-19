"""
dag_ai_sentiment.py — Airflow DAG orchestrating daily financial news ingestion and AI sentiment analysis.
"""
import os
import json
from datetime import datetime, timedelta
from loguru import logger

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    # Fallbacks for non-airflow testing environments
    DAG = None
    PythonOperator = None

# ─── Task Helpers ─────────────────────────────────────────────────────────────

def fetch_and_analyze_news(**kwargs):
    """
    Fetch market news headlines and analyze them via SentimentEngine.
    Saves results directly to S3/MinIO Bronze zone.
    """
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src")))
    from ai.sentiment_engine import SentimentEngine
    from storage.minio.client import MinIOClient
    from storage.minio.bronze import BronzeWriter

    # Financial mock feed headlines for standard daily run
    headlines = [
        "Apple reports record Q3 earnings beat, stock price climbs 2% in premarket trade",
        "Tesla stock price drops lower following delivery warning and analyst downgrade",
        "Nvidia rallies as growth outlook surges higher on strong AI hardware demand",
        "Regulatory warning pushes Meta Platforms stock lower amid privacy loss fears",
        "Amazon outpaces estimates, stock gains on strong cloud growth",
        "Google stock rises on search gains while warning of higher capital expenses",
    ]

    engine = SentimentEngine()
    results = []
    
    for h in headlines:
        analysis = engine.analyze_headline(h)
        results.append({
            "headline": h,
            "sentiment": analysis["sentiment"],
            "score": analysis["score"],
            "tickers": analysis["tickers"],
            "timestamp": datetime.utcnow().isoformat()
        })

    # Write to Bronze zone
    # Simulate S3 path writing
    try:
        minio_client = MinIOClient()
        writer = BronzeWriter(client=minio_client)
        
        batch_id = f"sentiment-batch-{int(datetime.utcnow().timestamp())}"
        path = writer.write_json_batch(
            source="sentiment",
            events=results,
            batch_id=batch_id
        )
        logger.info(f"AI news sentiment batch successfully written to S3 Bronze: {path}")
    except Exception as exc:
        logger.warning(f"Could not write to S3, saving locally: {exc}")
        # Fallback to local files for testing
        local_dir = "logs/bronze/sentiment"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, f"{batch_id}.json")
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        logger.info(f"AI news sentiment batch saved locally: {local_path}")

def run_sentiment_aggregation(**kwargs):
    """Trigger the Spark sentiment daily aggregator."""
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../src")))
    from transformation.spark.sentiment_aggregator import SentimentAggregator

    aggregator = SentimentAggregator()
    out_dir = aggregator.run_aggregation()
    logger.info(f"Spark aggregation finished: {out_dir}")

# ─── DAG Definition ───────────────────────────────────────────────────────────

if DAG:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        "dag_ai_sentiment_analysis",
        default_args=default_args,
        description="Daily financial news ingestion, sentiment analysis, and stock metrics correlations.",
        schedule_interval="@daily",
        start_date=datetime(2024, 6, 1),
        catchup=False,
    ) as dag:

        t1 = PythonOperator(
            task_id="fetch_and_analyze_news_feeds",
            python_callable=fetch_and_analyze_news,
        )

        t2 = PythonOperator(
            task_id="spark_aggregate_sentiment_correlations",
            python_callable=run_sentiment_aggregation,
        )

        t1 >> t2
else:
    # Enable test imports without failing
    pass
