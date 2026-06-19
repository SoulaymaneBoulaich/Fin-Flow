"""
streaming_consumer.py — Spark Structured Streaming: Kafka → Bronze.

This job reads from the 'stock-ticks' Kafka topic in real time and writes
raw JSON events to the Bronze zone in MinIO. It uses micro-batch mode with
a 5-second trigger interval.

Features:
 - Watermarking for late data (10-minute tolerance)
 - Append-only output (immutable Bronze)
 - Checkpoint location in MinIO for fault tolerance
 - Kafka offset tracking via consumer group
"""
from __future__ import annotations

import os
import signal
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)
from loguru import logger


MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY","finflow_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY","FinFlow_Secret_2024!")
BUCKET           = os.getenv("MINIO_BUCKET",     "finflow")

KAFKA_BROKERS    = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9093,kafka-3:9094")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock-ticks")

BRONZE_PATH      = f"s3a://{BUCKET}/bronze/stock-ticks-stream/"
CHECKPOINT_PATH  = f"s3a://{BUCKET}/checkpoints/stock-ticks-stream/"

# Schema of the JSON payload inside Kafka messages
TICK_SCHEMA = StructType([
    StructField("event_id",  StringType(),  True),
    StructField("ticker",    StringType(),  True),
    StructField("timestamp", StringType(),  True),
    StructField("open",      DoubleType(),  True),
    StructField("high",      DoubleType(),  True),
    StructField("low",       DoubleType(),  True),
    StructField("close",     DoubleType(),  True),
    StructField("volume",    LongType(),    True),
    StructField("vwap",      DoubleType(),  True),
    StructField("source",    StringType(),  True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinFlow-Streaming-Bronze")
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    """Start the streaming job. Blocks until terminated."""

    # ── Read from Kafka ────────────────────────────────────────────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)  # Backpressure: max 10K msgs/trigger
        .load()
    )

    # ── Parse JSON payload ─────────────────────────────────────────────────────
    parsed_df = kafka_df.select(
        F.from_json(
            F.col("value").cast(StringType()),
            TICK_SCHEMA,
        ).alias("data"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("offset"),
        F.col("partition"),
    ).select("data.*", "kafka_ts", "offset", "partition")

    # ── Apply watermark for late data tolerance ────────────────────────────────
    watermarked_df = parsed_df.withWatermark("kafka_ts", "10 minutes")

    # ── Add partition columns for file organization ────────────────────────────
    enriched_df = watermarked_df \
        .withColumn("timestamp_parsed", F.to_timestamp("timestamp")) \
        .withColumn("year",  F.year("timestamp_parsed")) \
        .withColumn("month", F.month("timestamp_parsed")) \
        .withColumn("day",   F.dayofmonth("timestamp_parsed")) \
        .withColumn("hour",  F.hour("timestamp_parsed")) \
        .drop("timestamp_parsed")

    # ── Write to Bronze (append only) ─────────────────────────────────────────
    query = (
        enriched_df.writeStream
        .format("json")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("path", BRONZE_PATH)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info(
        f"Streaming query started: {query.name}. "
        f"Reading from topic='{KAFKA_TOPIC}', writing to '{BRONZE_PATH}'"
    )

    def _shutdown(sig, frame):
        logger.info("Received shutdown signal. Stopping streaming query...")
        query.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    query.awaitTermination()
    logger.info("Streaming query terminated.")


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run(spark)
    finally:
        spark.stop()
