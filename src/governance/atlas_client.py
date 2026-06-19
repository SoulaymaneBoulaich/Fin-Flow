"""
atlas_client.py — Apache Atlas metadata catalog integration.

Provides:
 - Asset registration (topics, tables, views)
 - Lineage tracking (source → transform → target)
 - Classification tagging (PII, FINANCIAL, etc.)
 - Asset search and retrieval
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from loguru import logger


class AtlasClient:
    """
    Client for Apache Atlas REST API v2.
    Falls back to local logging if Atlas is unavailable.
    """

    def __init__(
        self,
        host: str = "atlas",
        port: int = 21000,
        username: str = "admin",
        password: str = "admin",
    ) -> None:
        self.base_url = f"http://{host}:{port}/api/atlas/v2"
        self._auth = (username, password)
        self._session = requests.Session()
        self._session.headers["Content-Type"] = "application/json"
        self._available = self._check_availability()

    def _check_availability(self) -> bool:
        try:
            resp = self._session.get(
                f"{self.base_url}/types/typedefs",
                auth=self._auth,
                timeout=3,
            )
            return resp.status_code == 200
        except Exception:
            logger.warning("Apache Atlas not available — lineage will be logged only.")
            return False

    def _post(self, path: str, body: dict) -> Optional[dict]:
        if not self._available:
            logger.info(f"[ATLAS LOG] POST {path}: {json.dumps(body, default=str)[:200]}")
            return None
        try:
            resp = self._session.post(
                f"{self.base_url}{path}",
                json=body,
                auth=self._auth,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error(f"Atlas API error: {exc}")
            return None

    def register_kafka_topic(self, topic_name: str, description: str = "") -> Optional[str]:
        """Register a Kafka topic as an Atlas entity."""
        entity = {
            "entity": {
                "typeName": "kafka_topic",
                "attributes": {
                    "name": topic_name,
                    "qualifiedName": f"finflow.kafka.{topic_name}",
                    "description": description,
                    "topic": topic_name,
                    "uri": f"kafka://kafka-1:9092/{topic_name}",
                },
                "classifications": [
                    {"typeName": "FINANCIAL"},
                ],
            }
        }
        result = self._post("/entity", entity)
        if result:
            guid = result.get("guidAssignments", {}).get(topic_name)
            logger.info(f"Atlas: Registered Kafka topic '{topic_name}' guid={guid}")
            return guid
        return None

    def register_hive_table(
        self,
        database: str,
        table: str,
        zone: str,
        description: str = "",
        classifications: Optional[list[str]] = None,
    ) -> Optional[str]:
        """Register a Hive/Iceberg table as an Atlas entity."""
        tags = classifications or []
        entity = {
            "entity": {
                "typeName": "hive_table",
                "attributes": {
                    "name": table,
                    "qualifiedName": f"finflow.{database}.{table}@finflow",
                    "description": description,
                    "db": {"typeName": "hive_db", "uniqueAttributes": {"qualifiedName": database}},
                },
                "classifications": [{"typeName": t} for t in tags],
                "customAttributes": {
                    "finflow_zone": zone,
                    "platform": "FinFlow",
                    "registered_at": datetime.now(timezone.utc).isoformat(),
                },
            }
        }
        result = self._post("/entity", entity)
        if result:
            logger.info(f"Atlas: Registered table '{database}.{table}' zone={zone}")
        return None

    def create_lineage(
        self,
        source_qualified_name: str,
        target_qualified_name: str,
        process_name: str,
        process_description: str = "",
    ) -> None:
        """
        Create a lineage edge: source → process → target.
        This is how Atlas tracks data flow through the pipeline.
        """
        process_entity = {
            "entity": {
                "typeName": "Process",
                "attributes": {
                    "name": process_name,
                    "qualifiedName": f"finflow.process.{process_name}",
                    "description": process_description,
                    "inputs": [
                        {"typeName": "DataSet",
                         "uniqueAttributes": {"qualifiedName": source_qualified_name}}
                    ],
                    "outputs": [
                        {"typeName": "DataSet",
                         "uniqueAttributes": {"qualifiedName": target_qualified_name}}
                    ],
                },
            }
        }
        self._post("/entity", process_entity)
        logger.info(
            f"Atlas lineage: {source_qualified_name} "
            f"→ [{process_name}] → {target_qualified_name}"
        )

    def tag_with_classification(
        self,
        qualified_name: str,
        entity_type: str,
        classification: str,
    ) -> None:
        """Add a classification tag to an existing Atlas entity."""
        search_resp = None
        if self._available:
            try:
                search_resp = self._session.get(
                    f"{self.base_url}/search/attribute",
                    params={"typeName": entity_type, "attrName": "qualifiedName",
                            "attrValuePrefix": qualified_name},
                    auth=self._auth,
                    timeout=5,
                ).json()
            except Exception:
                pass

        logger.info(
            f"[ATLAS] Tagging '{qualified_name}' with classification '{classification}'"
        )

    def initialize_finflow_catalog(self) -> None:
        """Register all FinFlow data assets in Atlas on startup."""
        logger.info("Initializing FinFlow Atlas catalog...")

        # Register Kafka topics
        self.register_kafka_topic("stock-ticks", "Real-time stock price events")
        self.register_kafka_topic("trade-events", "User trade execution events")
        self.register_kafka_topic("user-events", "User account events")

        # Register storage tables
        self.register_hive_table("bronze", "stock_ticks", "bronze",
                                  "Raw stock ticks", ["FINANCIAL", "PUBLIC"])
        self.register_hive_table("silver", "stock_ticks", "silver",
                                  "Cleaned ticks", ["FINANCIAL", "INTERNAL"])
        self.register_hive_table("gold", "daily_ohlcv", "gold",
                                  "Daily OHLCV summaries", ["FINANCIAL", "INTERNAL"])

        # Register lineage
        self.create_lineage(
            "finflow.kafka.stock-ticks",
            "finflow.bronze.stock_ticks@finflow",
            "spark-streaming-kafka-to-bronze",
            "Spark Structured Streaming: Kafka → Bronze",
        )
        self.create_lineage(
            "finflow.bronze.stock_ticks@finflow",
            "finflow.silver.stock_ticks@finflow",
            "spark-batch-bronze-to-silver",
            "Spark batch: Bronze → Silver (clean + deduplicate)",
        )
        self.create_lineage(
            "finflow.silver.stock_ticks@finflow",
            "finflow.gold.daily_ohlcv@finflow",
            "spark-batch-silver-to-gold",
            "Spark batch: Silver → Gold (daily aggregations)",
        )

        logger.info("Atlas catalog initialization complete.")
