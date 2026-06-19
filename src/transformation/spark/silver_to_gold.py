"""
silver_to_gold.py — Spark batch job: Silver → Gold aggregations.

Computes per-ticker, per-day:
 - Daily OHLCV summary
 - 7/14/30-day Simple Moving Averages
 - Annualized volatility (std dev of log returns × √252)
 - VWAP (Volume-Weighted Average Price)

Run:
  spark-submit --master spark://spark-master:7077 \
    src/transformation/spark/silver_to_gold.py
"""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from loguru import logger


MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","finflow_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","FinFlow_Secret_2024!")
BUCKET           = os.getenv("MINIO_BUCKET",     "finflow")
SILVER_PATH      = f"s3a://{BUCKET}/silver/stock-ticks/"
GOLD_PATH        = f"s3a://{BUCKET}/gold/daily-summary/"


def build_spark_session(app_name: str = "FinFlow-Silver-to-Gold") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled",             "true")
        .config("spark.sql.shuffle.partitions",           "20")
        .getOrCreate()
    )


def read_silver(spark: SparkSession) -> DataFrame:
    """Read all Silver Parquet files."""
    df = spark.read.parquet(SILVER_PATH)
    logger.info(f"Silver records loaded: {df.count()}")
    return df


def compute_daily_ohlcv(df: DataFrame) -> DataFrame:
    """
    Compute daily OHLCV per ticker.

    For VWAP: weighted average close = sum(close * volume) / sum(volume)
    """
    # Sort window: first/last open/close require ordered aggregation
    # We approximate first_open and last_close using min/max timestamp tricks
    df_with_date = df.withColumn("date", F.to_date("timestamp"))

    # Window to get first and last close per ticker/day
    w_asc  = Window.partitionBy("ticker", "date").orderBy(F.col("timestamp").asc())
    w_desc = Window.partitionBy("ticker", "date").orderBy(F.col("timestamp").desc())

    df_ranked = df_with_date \
        .withColumn("_rn_asc",  F.row_number().over(w_asc)) \
        .withColumn("_rn_desc", F.row_number().over(w_desc))

    daily = df_with_date.groupBy("ticker", "date").agg(
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.sum("volume").alias("volume"),
        F.count("*").alias("tick_count"),
        # VWAP numerator
        F.sum(F.col("close") * F.col("volume")).alias("pv_sum"),
        F.sum("volume").alias("total_volume"),
    ).withColumn(
        "vwap",
        F.when(F.col("total_volume") > 0,
               F.round(F.col("pv_sum") / F.col("total_volume"), 4)
        ).otherwise(0.0)
    )

    # Get first open per ticker/day
    first_open = df_ranked.filter(F.col("_rn_asc") == 1) \
        .select("ticker", "date", F.col("open").alias("open"))

    # Get last close per ticker/day
    last_close = df_ranked.filter(F.col("_rn_desc") == 1) \
        .select("ticker", "date", F.col("close").alias("close"))

    result = daily \
        .join(first_open, on=["ticker", "date"], how="left") \
        .join(last_close, on=["ticker", "date"], how="left") \
        .drop("pv_sum", "total_volume")

    return result


def compute_moving_averages(daily_df: DataFrame) -> DataFrame:
    """
    Compute 7-day, 14-day, and 30-day Simple Moving Averages.
    Uses a row-based rolling window sorted by date per ticker.
    """
    w7  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-6, 0)
    w14 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-13, 0)
    w30 = Window.partitionBy("ticker").orderBy("date").rowsBetween(-29, 0)

    return daily_df \
        .withColumn("ma_7",  F.round(F.avg("close").over(w7),  4)) \
        .withColumn("ma_14", F.round(F.avg("close").over(w14), 4)) \
        .withColumn("ma_30", F.round(F.avg("close").over(w30), 4))


def compute_volatility(daily_df: DataFrame, window_days: int = 30) -> DataFrame:
    """
    Annualized volatility = std(log_returns) * sqrt(252).
    log_return_t = ln(close_t / close_{t-1})
    """
    w_prev = Window.partitionBy("ticker").orderBy("date")
    w_vol  = Window.partitionBy("ticker").orderBy("date").rowsBetween(-(window_days - 1), 0)

    df_with_log_return = daily_df.withColumn(
        "prev_close", F.lag("close", 1).over(w_prev)
    ).withColumn(
        "log_return",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            F.log(F.col("close") / F.col("prev_close"))
        ).otherwise(F.lit(None))
    )

    df_with_vol = df_with_log_return.withColumn(
        "volatility_annualized",
        F.round(F.stddev("log_return").over(w_vol) * (252 ** 0.5), 6)
    ).drop("prev_close", "log_return")

    return df_with_vol


def add_gold_metadata(df: DataFrame) -> DataFrame:
    """Add audit metadata to the Gold table."""
    return df \
        .withColumn("computed_at", F.current_timestamp()) \
        .withColumn("pipeline_version", F.lit("1.0.0"))


def write_gold(df: DataFrame) -> None:
    """Write Gold summaries as Parquet, partitioned by ticker/year/month."""
    df_partitioned = df \
        .withColumn("year",  F.year(F.col("date"))) \
        .withColumn("month", F.month(F.col("date")))

    logger.info(f"Writing Gold to: {GOLD_PATH}")
    (
        df_partitioned.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("ticker", "year", "month")
        .option("compression", "snappy")
        .save(GOLD_PATH)
    )
    logger.info("Gold write complete.")


def run(spark: SparkSession) -> dict:
    """Full Silver → Gold pipeline."""
    start_time = datetime.now()

    silver_df = read_silver(spark)
    records_in = silver_df.count()

    daily_df   = compute_daily_ohlcv(silver_df)
    ma_df      = compute_moving_averages(daily_df)
    vol_df     = compute_volatility(ma_df)
    final_df   = add_gold_metadata(vol_df)
    records_out = final_df.count()

    write_gold(final_df)

    duration = (datetime.now() - start_time).total_seconds()
    metrics = {
        "records_in": records_in,
        "records_out": records_out,
        "duration_seconds": duration,
    }
    logger.info(f"Silver→Gold complete: {metrics}")
    return metrics


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run(spark)
    finally:
        spark.stop()
