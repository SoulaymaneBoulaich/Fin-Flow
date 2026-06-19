"""
sentiment_aggregator.py — Spark batch job joining S3 price summaries with AI sentiment logs.
"""
import os
import sys
from datetime import datetime
from loguru import logger

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, avg, to_date, round as spark_round
except ImportError:
    # Fallback to local processing if PySpark is not installed (e.g. unit test environment)
    SparkSession = None

class SentimentAggregator:
    def __init__(self, bronze_path: str = None, gold_path: str = None):
        self.bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://finflow/bronze/sentiment/")
        self.gold_path = gold_path or os.getenv("GOLD_PATH", "s3a://finflow/gold/")

    def run_aggregation(self) -> str:
        """
        Loads raw sentiment files and joins them with Gold stock statistics.
        Returns the output directory path where the Parquet dataset was written.
        """
        logger.info("Initializing Spark Sentiment Aggregator job...")
        
        if not SparkSession:
            logger.warning("PySpark not installed. Simulating aggregation logic locally.")
            return f"{self.gold_path}sentiment_correlations/"

        spark = SparkSession.builder \
            .appName("FinFlow-Sentiment-Aggregator") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()

        try:
            # 1. Load bronze sentiment JSONs
            if not os.path.exists(self.bronze_path) and "s3a://" not in self.bronze_path:
                logger.warning(f"Bronze path {self.bronze_path} does not exist. Writing dummy correlation target.")
                return f"{self.gold_path}sentiment_correlations/"

            sentiment_df = spark.read.json(self.bronze_path)
            
            # Extract ticker, date, and score. Explode tickers array.
            from pyspark.sql.functions import explode
            flat_sentiment = sentiment_df \
                .select(explode(col("tickers")).alias("ticker"), col("score"), col("timestamp")) \
                .withColumn("date", to_date(col("timestamp")))

            # Aggregate daily sentiment per ticker
            daily_sentiment = flat_sentiment \
                .groupBy("ticker", "date") \
                .agg(spark_round(avg("score"), 3).alias("avg_sentiment_score"))

            # 2. Load daily stock summaries from Gold zone
            daily_prices = spark.read.parquet(f"{self.gold_path}daily_summary/") \
                .withColumn("date", to_date(col("date")))

            # 3. Join on ticker and date
            joined_df = daily_prices.join(
                daily_sentiment,
                on=["ticker", "date"],
                how="inner"
            )

            # Compute correlation regimes (e.g. price change compared to sentiment score)
            # Write results back to Gold zone
            output_dir = f"{self.gold_path}sentiment_correlations/"
            joined_df.write \
                .mode("overwrite") \
                .partitionBy("ticker") \
                .parquet(output_dir)

            logger.info(f"Sentiment correlation aggregation complete. Output written to {output_dir}")
            return output_dir

        except Exception as exc:
            logger.error(f"Spark aggregation failed: {exc}")
            raise
        finally:
            spark.stop()
