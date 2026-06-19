"""
admin.py — Kafka Admin client for topic creation, listing, and management.
"""
from __future__ import annotations

import os
import json
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
from loguru import logger


class KafkaAdmin:
    """Wrapper around confluent-kafka AdminClient."""

    def __init__(self, bootstrap_servers: Optional[str] = None) -> None:
        servers = bootstrap_servers or os.getenv(
            "KAFKA_BROKERS",
            "localhost:29092,localhost:29093,localhost:29094",
        )
        self._admin = AdminClient({"bootstrap.servers": servers})
        logger.info(f"KafkaAdmin connected to: {servers}")

    def create_topic(
        self,
        name: str,
        num_partitions: int = 5,
        replication_factor: int = 3,
        config: Optional[dict] = None,
    ) -> bool:
        """
        Create a Kafka topic. Returns True if created, False if already exists.
        Raises KafkaException on other errors.
        """
        topic_config = config or {}
        new_topic = NewTopic(
            topic=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=topic_config,
        )

        futures = self._admin.create_topics([new_topic])

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic created: {topic} (partitions={num_partitions})")
                return True
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    logger.debug(f"Topic already exists: {topic}")
                    return False
                raise KafkaException(exc) from exc

    def list_topics(self) -> list[str]:
        """List all Kafka topics (excluding internal __ topics)."""
        metadata = self._admin.list_topics(timeout=10)
        return [
            t for t in metadata.topics.keys()
            if not t.startswith("__")
        ]

    def delete_topic(self, name: str) -> None:
        """Delete a Kafka topic."""
        futures = self._admin.delete_topics([name])
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic deleted: {topic}")
            except Exception as exc:
                logger.error(f"Failed to delete topic {topic}: {exc}")
                raise

    def describe_topic(self, name: str) -> dict:
        """Return partition and replica information for a topic."""
        metadata = self._admin.list_topics(topic=name, timeout=10)
        if name not in metadata.topics:
            raise ValueError(f"Topic '{name}' not found")

        topic_meta = metadata.topics[name]
        return {
            "name": name,
            "partitions": len(topic_meta.partitions),
            "partition_details": [
                {
                    "id": p.id,
                    "leader": p.leader,
                    "replicas": list(p.replicas),
                    "isrs": list(p.isrs),
                }
                for p in topic_meta.partitions.values()
            ],
        }

    def initialize_from_config(self, config_path: str) -> None:
        """Create all topics defined in a topics.json config file."""
        with open(config_path) as f:
            config = json.load(f)

        for topic_def in config.get("topics", []):
            self.create_topic(
                name=topic_def["name"],
                num_partitions=topic_def.get("partitions", 5),
                replication_factor=topic_def.get("replicationFactor", 3),
                config=topic_def.get("config", {}),
            )
        logger.info(f"Initialized {len(config['topics'])} topics from {config_path}")

    def close(self) -> None:
        """The AdminClient has no explicit close, but log for consistency."""
        logger.debug("KafkaAdmin session ended.")
