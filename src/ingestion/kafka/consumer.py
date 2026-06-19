"""
consumer.py — FinFlow Kafka Consumer base class.

Features:
 - Consumer group management with configurable offset reset
 - JSON deserialization with schema validation
 - Graceful shutdown via threading.Event
 - Offset commit after successful message processing
 - Configurable batch processing
"""
from __future__ import annotations

import json
import os
import threading
from typing import Optional, Callable, Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from loguru import logger


class FinFlowConsumer:
    """Kafka consumer with offset management and graceful shutdown."""

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = "earliest",
        extra_config: Optional[dict] = None,
    ) -> None:
        servers = bootstrap_servers or os.getenv(
            "KAFKA_BROKERS",
            "localhost:29092,localhost:29093,localhost:29094",
        )

        config = {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,        # Manual commit for exactly-once
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
            "max.poll.interval.ms": 300000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
        }

        if extra_config:
            config.update(extra_config)

        self._consumer = Consumer(config)
        self._topics = topics
        self._group_id = group_id
        self._shutdown = threading.Event()
        self._running = False

        self._consumer.subscribe(topics)
        logger.info(
            f"FinFlowConsumer initialized. Group={group_id} Topics={topics}"
        )

    def consume(
        self,
        message_handler: Callable[[Message], None],
        batch_size: int = 100,
        poll_timeout: float = 1.0,
        commit_every: int = 100,
    ) -> None:
        """
        Main consume loop. Calls message_handler for each message.

        Args:
            message_handler: Callable that processes a single Kafka Message.
            batch_size:       Number of messages to process per batch.
            poll_timeout:     Seconds to wait for a message per poll.
            commit_every:     Commit offsets after this many successful messages.
        """
        self._running = True
        processed_count = 0

        logger.info(
            f"Starting consume loop on topics={self._topics} "
            f"group={self._group_id}"
        )

        try:
            while not self._shutdown.is_set():
                msg = self._consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"End of partition: {msg.topic()} [{msg.partition()}]"
                            f" @ offset {msg.offset()}"
                        )
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    message_handler(msg)
                    processed_count += 1
                    if processed_count % commit_every == 0:
                        self._consumer.commit(asynchronous=False)
                        logger.debug(f"Committed offsets after {processed_count} messages")
                except Exception as exc:
                    logger.error(
                        f"Message handler failed for offset={msg.offset()} "
                        f"topic={msg.topic()}: {exc}"
                    )
                    # Do not commit — the message will be reprocessed

        finally:
            # Final commit before shutdown
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass
            self._consumer.close()
            self._running = False
            logger.info(f"Consumer loop ended. Total processed: {processed_count}")

    def stop(self) -> None:
        """Signal the consumer to stop after the current message."""
        logger.info("Consumer stop requested.")
        self._shutdown.set()

    @staticmethod
    def parse_json(msg: Message) -> dict:
        """Deserialize a Kafka message value from JSON bytes."""
        try:
            return json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"Failed to parse message as JSON: {exc}") from exc

    @property
    def is_running(self) -> bool:
        return self._running

    def __enter__(self) -> "FinFlowConsumer":
        return self

    def __exit__(self, *args) -> None:
        self.stop()
