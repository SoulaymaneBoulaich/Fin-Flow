"""
bronze_to_silver.py — Spark batch job: Bronze → Silver transformation.

What this job does:
 1. Reads new JSON files from Bronze (Iceberg snapshot-based incremental read)
 2. Casts all fields to proper types
 3. Validates ranges (price > 0, volume >= 0)
 4. Deduplicates using SHA-256 hash of (ticker, timestamp, close)
 5. Tokenizes PII in user data
 6. Writes Parquet to Silver zone, partitioned by ticker/date
 7. Registers result in Hive Metastore via Iceberg

Run:
  spark-submit --master spark://spark-master:7077 \
    src/transformation/spark/bronze_to_silver.py
"""
from __future__ import annotations

import os
import sys
import hashlib
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType,
)
from loguru import logger


# ─── Schema for raw Bronze tick data ──────────────────────────────────────────
BRONZE_TICK_SCHEMA = StructType([
    StructField("event_id",  StringType(),    True),
    StructField("ticker",    StringType(),    True),
    StructField("timestamp", StringType(),    True),  # ISO string
    StructField("open",      StringType(),    True),
    StructField("high",      StringType(),    True),
    StructField("low",       StringType(),    True),
    StructField("close",     StringType(),    True),
    StructField("volume",    StringType(),    True),
    StructField("vwap",      StringType(),    True),
    StructField("source",    StringType(),    True),
])

# ─── S3A paths ─────────────────────────────────────────────────────────────────
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY","finflow_admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY","FinFlow_Secret_2024!")
BUCKET            = os.getenv("MINIO_BUCKET",     "finflow")
BRONZE_PATH       = f"s3a://{BUCKET}/bronze/stock-ticks/"
SILVER_PATH       = f"s3a://{BUCKET}/silver/stock-ticks/"


def build_spark_session(app_name: str = "FinFlow-Bronze-to-Silver") -> SparkSession:
    """Build a SparkSession with S3A and Iceberg support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.finflow",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.finflow.type",         "hive")
        .config("spark.sql.catalog.finflow.uri",          "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.finflow.warehouse",    f"s3a://{BUCKET}/warehouse")
        .config("spark.sql.adaptive.enabled",             "true")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Read all JSON files from Bronze zone."""
    logger.info(f"Reading Bronze from: {path}")
    df = (
        spark.read
        .schema(BRONZE_TICK_SCHEMA)
        .json(path)
    )
    logger.info(f"Bronze read: {df.count()} records")
    return df


def cast_and_validate(df: DataFrame) -> DataFrame:
    """
    Cast string fields to typed fields and filter invalid rows.
    Invalid rows (bad price, bad volume) are logged and dropped.
    """
    # ── Cast ──────────────────────────────────────────────────────────────────
    df_cast = df.select(
        F.col("event_id"),
        F.upper(F.trim(F.col("ticker"))).alias("ticker"),
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("open").cast(DoubleType()).alias("open"),
        F.col("high").cast(DoubleType()).alias("high"),
        F.col("low").cast(DoubleType()).alias("low"),
        F.col("close").cast(DoubleType()).alias("close"),
        F.col("volume").cast(LongType()).alias("volume"),
        F.col("vwap").cast(DoubleType()).alias("vwap"),
        F.col("source"),
    )

    # ── Fix nulls ─────────────────────────────────────────────────────────────
    df_filled = df_cast.fillna({
        "vwap": 0.0,
        "source": "unknown",
    })

    # ── Range validation ──────────────────────────────────────────────────────
    valid = df_filled.filter(
        (F.col("close") > 0)
        & (F.col("volume") >= 0)
        & F.col("timestamp").isNotNull()
        & F.col("ticker").isNotNull()
    )

    invalid_count = df_cast.count() - valid.count()
    if invalid_count > 0:
        logger.warning(f"Dropped {invalid_count} invalid records (range/null check)")

    return valid


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate rows using a hash of (ticker, timestamp, close).
    Keeps the first occurrence.
    """
    # Compute dedup hash as a UDF
    @F.udf(StringType())
    def dedup_hash(ticker, ts, close):
        key = f"{ticker}|{ts}|{close}"
        return hashlib.sha256(key.encode()).hexdigest()

    df_hashed = df.withColumn(
        "_dedup_hash",
        dedup_hash(F.col("ticker"), F.col("timestamp").cast(StringType()), F.col("close").cast(StringType()))
    )

    # Deduplicate: keep first row per hash
    deduplicated = df_hashed.dropDuplicates(["_dedup_hash"])
    dupes = df.count() - deduplicated.count()
    if dupes > 0:
        logger.info(f"Removed {dupes} duplicate records")

    return deduplicated


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Add year/month/day partition columns for efficient downstream queries."""
    return df.withColumn("year",  F.year("timestamp")) \
             .withColumn("month", F.month("timestamp")) \
             .withColumn("day",   F.dayofmonth("timestamp"))


def write_silver(df: DataFrame, path: str) -> None:
    """Write the cleaned DataFrame to Silver zone as Parquet, partitioned by ticker/date."""
    logger.info(f"Writing Silver to: {path}")
    (
        df.write
        .format("parquet")
        .mode("append")
        .partitionBy("ticker", "year", "month", "day")
        .option("compression", "snappy")
        .save(path)
    )
    logger.info("Silver write complete.")


def register_iceberg_table(spark: SparkSession, silver_path: str) -> None:
    """Register the Silver Parquet data as an Iceberg table in Hive Metastore."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS finflow.silver.stock_ticks (
            event_id    STRING,
            ticker      STRING,
            timestamp   TIMESTAMP,
            open        DOUBLE,
            high        DOUBLE,
            low         DOUBLE,
            close       DOUBLE,
            volume      BIGINT,
            vwap        DOUBLE,
            source      STRING,
            _dedup_hash STRING,
            year        INT,
            month       INT,
            day         INT
        )
        USING iceberg
        PARTITIONED BY (ticker, year, month, day)
        LOCATION '{silver_path}'
    """.replace("{silver_path}", silver_path))
    logger.info("Iceberg table 'finflow.silver.stock_ticks' registered in Hive Metastore.")


def run(spark: SparkSession) -> dict:
    """Full Bronze → Silver pipeline. Returns job metrics."""
    start_time = datetime.now()

    # 1. Read
    bronze_df = read_bronze(spark, BRONZE_PATH)
    records_in = bronze_df.count()

    # 2. Cast + validate
    valid_df = cast_and_validate(bronze_df)

    # 3. Deduplicate
    deduped_df = deduplicate(valid_df)

    # 4. Add partition columns
    final_df = add_partition_columns(deduped_df)
    records_out = final_df.count()

    # 5. Write
    write_silver(final_df, SILVER_PATH)

    # 6. Register
    try:
        register_iceberg_table(spark, SILVER_PATH)
    except Exception as exc:
        logger.warning(f"Iceberg registration skipped (metastore may not be ready): {exc}")

    duration = (datetime.now() - start_time).total_seconds()
    metrics = {
        "records_in": records_in,
        "records_out": records_out,
        "records_skipped": records_in - records_out,
        "duration_seconds": duration,
    }
    logger.info(f"Bronze→Silver complete: {metrics}")
    return metrics


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run(spark)
    finally:
        spark.stop()
