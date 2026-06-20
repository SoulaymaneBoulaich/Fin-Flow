"""
main.py — Entry point for the data generation container.
Orchestrates: user initialization → stock producer → trade simulator
"""
from __future__ import annotations

import os
import sys
import signal
import threading
import time

from loguru import logger

sys.path.insert(0, "/app/src")

from generation.synthetic_users import initialize_users
from generation.stock_producer import main as run_stock_producer, _shutdown as stock_shutdown
from generation.trade_simulator import main as run_trade_simulator, _shutdown as trade_shutdown
from generation.cassandra_consumer import run_consumer as run_cassandra_consumer, _shutdown as cassandra_shutdown
from ingestion.kafka.producer import FinFlowProducer

BROKERS: str = ",".join([
    os.getenv("KAFKA_BROKER_1", "kafka-1:9092"),
    os.getenv("KAFKA_BROKER_2", "kafka-2:9093"),
    os.getenv("KAFKA_BROKER_3", "kafka-3:9094"),
])

_global_shutdown = threading.Event()


def _signal_handler(sig, frame):
    logger.info(f"Signal {sig}: shutting down all generators...")
    _global_shutdown.set()
    stock_shutdown.set()
    trade_shutdown.set()
    cassandra_shutdown.set()


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("=== FinFlow Data Generation Service Starting ===")

    # Wait for Kafka to be ready
    logger.info("Waiting 30s for Kafka cluster to be fully ready...")
    time.sleep(30)

    # Step 1: Initialize synthetic users
    producer = FinFlowProducer(bootstrap_servers=BROKERS)
    users = initialize_users(producer)
    logger.info(f"Step 1 complete: {len(users)} users initialized.")

    # Step 2: Start stock producer in background thread
    stock_thread = threading.Thread(
        target=run_stock_producer,
        name="stock-producer-main",
        daemon=True,
    )
    stock_thread.start()

    # Step 3: Start trade simulator in background thread
    trade_thread = threading.Thread(
        target=run_trade_simulator,
        name="trade-simulator-main",
        daemon=True,
    )
    trade_thread.start()

    # Step 4: Start Cassandra consumer in background thread
    cassandra_thread = threading.Thread(
        target=run_cassandra_consumer,
        name="cassandra-consumer-main",
        daemon=True,
    )
    cassandra_thread.start()

    logger.info("All data generators running. Press Ctrl+C to stop.")
    _global_shutdown.wait()

    logger.info("All generators stopped.")


if __name__ == "__main__":
    main()
