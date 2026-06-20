"""
cassandra_consumer.py — Consumes tick events from Kafka and writes them to Cassandra.
"""
from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timezone
from loguru import logger

# Add parent path to import components
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.kafka.consumer import FinFlowConsumer
from storage.cassandra.client import CassandraClient
from storage.cassandra.tick_store import TickStore

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9093,kafka-3:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock-ticks")

_shutdown = threading.Event()


def run_consumer() -> None:
    logger.info("Initializing Cassandra client...")
    try:
        client = CassandraClient(hosts=[CASSANDRA_HOST])
        # Connect to Cassandra
        client.connect()
        store = TickStore(client)
        logger.info("Cassandra client initialized successfully.")
    except Exception as exc:
        logger.error(f"Failed to connect to Cassandra at host {CASSANDRA_HOST}: {exc}")
        return

    logger.info("Initializing Kafka consumer...")
    try:
        consumer = FinFlowConsumer(
            topics=[KAFKA_TOPIC],
            group_id="cassandra-tick-consumer",
            bootstrap_servers=KAFKA_BROKERS,
        )
    except Exception as exc:
        logger.error(f"Failed to initialize Kafka consumer: {exc}")
        client.close()
        return

    def _message_handler(msg) -> None:
        try:
            data = consumer.parse_json(msg)
            
            # Parse timestamp securely
            ts_str = data["timestamp"]
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1] + "+00:00"
            ts = datetime.fromisoformat(ts_str)

            store.insert_tick(
                ticker=data["ticker"],
                timestamp=ts,
                event_id=data["event_id"],
                open_=float(data["open"]),
                high=float(data["high"]),
                low=float(data["low"]),
                close=float(data["close"]),
                volume=int(data["volume"]),
                vwap=float(data.get("vwap") or data["close"]),
                source=data.get("source", "simulated"),
            )
            logger.debug(f"Cassandra Ingestion: Inserted {data['ticker']} close={data['close']} ({ts_str})")
        except Exception as exc:
            logger.error(f"Failed to process and store tick event: {exc}")

    # Set up thread exit monitoring
    def _monitor():
        while not _shutdown.is_set():
            _shutdown.wait(1.0)
        logger.info("Stopping Kafka Cassandra consumer...")
        consumer.stop()

    monitor_thread = threading.Thread(target=_monitor, name="cassandra-consumer-monitor", daemon=True)
    monitor_thread.start()

    logger.info("Starting consumption loop for Cassandra tick storage...")
    try:
        consumer.consume(message_handler=_message_handler, commit_every=10)
    except Exception as exc:
        logger.error(f"Cassandra consumer encountered loop error: {exc}")
    finally:
        client.close()
        logger.info("Cassandra client session closed.")


def main() -> None:
    run_consumer()


if __name__ == "__main__":
    main()
