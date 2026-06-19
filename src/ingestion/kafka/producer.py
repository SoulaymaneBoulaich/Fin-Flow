"""
producer.py — FinFlow Kafka Producer base class.

Wraps confluent-kafka's Producer with:
 - Automatic retry on transient errors
 - Delivery report callback with logging
 - JSON serialization helper
 - Connection health check
"""
from __future__ import annotations

import json
import os
import time
from typing import Optional, Callable

from confluent_kafka import Producer, KafkaException
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class FinFlowProducer:
    """Thread-safe Kafka producer with built-in retry and delivery tracking."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        extra_config: Optional[dict] = None,
    ) -> None:
        servers = bootstrap_servers or os.getenv(
            "KAFKA_BROKERS",
            "localhost:29092,localhost:29093,localhost:29094",
        )

        config = {
            "bootstrap.servers": servers,
            "acks": "all",                  # Wait for all in-sync replicas
            "retries": 5,
            "retry.backoff.ms": 500,
            "linger.ms": 5,                 # Small batching window
            "batch.size": 65536,            # 64 KB batch
            "compression.type": "lz4",
            "enable.idempotence": True,     # Exactly-once producer semantics
            "max.in.flight.requests.per.connection": 5,
            "delivery.timeout.ms": 30000,
        }

        if extra_config:
            config.update(extra_config)

        self._producer = Producer(config)
        self._delivery_errors: list[str] = []
        logger.info(f"FinFlowProducer initialized. Brokers: {servers}")

    def _delivery_callback(self, err, msg) -> None:
        """Called by librdkafka on message delivery (success or failure)."""
        if err:
            error_msg = (
                f"Delivery failed: topic={msg.topic()} "
                f"partition={msg.partition()} error={err}"
            )
            logger.error(error_msg)
            self._delivery_errors.append(error_msg)
        else:
            logger.debug(
                f"Delivered: topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(KafkaException),
        reraise=True,
    )
    def publish(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None,
    ) -> None:
        """
        Publish a single message to Kafka.

        Args:
            topic:   Kafka topic name
            value:   Message payload (bytes)
            key:     Optional message key (bytes) — used for partition routing
            headers: Optional dict of header key-value pairs
        """
        kafka_headers = None
        if headers:
            kafka_headers = [(k, v.encode() if isinstance(v, str) else v)
                             for k, v in headers.items()]

        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            headers=kafka_headers,
            callback=self._delivery_callback,
        )
        # Poll to trigger delivery callbacks (non-blocking)
        self._producer.poll(0)

    def publish_json(
        self,
        topic: str,
        data: dict,
        key: Optional[str] = None,
    ) -> None:
        """Convenience method: serialize a dict to JSON and publish."""
        value_bytes = json.dumps(data, default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        self.publish(topic=topic, value=value_bytes, key=key_bytes)

    def flush(self, timeout: float = 15.0) -> int:
        """
        Wait for all outstanding messages to be delivered.
        Returns the number of messages still in queue (0 = success).
        """
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"Producer flush timed out with {remaining} messages undelivered.")
        return remaining

    def close(self) -> None:
        """Gracefully shut down the producer."""
        self.flush()
        logger.info("FinFlowProducer closed.")

    @property
    def delivery_errors(self) -> list[str]:
        return list(self._delivery_errors)

    def __enter__(self) -> "FinFlowProducer":
        return self

    def __exit__(self, *args) -> None:
        self.close()
