"""
bronze.py — Bronze zone: raw, immutable data writes to MinIO.

Pattern: append-only, partitioned by year/month/day/hour.
Data is stored as-received (JSON bytes) with no transformation.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from storage.minio.client import MinIOClient


BRONZE_PREFIX = os.getenv("MINIO_BRONZE_PREFIX", "bronze")
BUCKET = os.getenv("MINIO_BUCKET", "finflow")


def _build_path(
    source: str,
    ts: datetime,
    filename: str,
) -> str:
    """
    Build a partitioned path for Bronze zone.
    Pattern: bronze/{source}/year={Y}/month={MM}/day={DD}/hour={HH}/{filename}
    """
    return (
        f"{BRONZE_PREFIX}/{source}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"hour={ts.hour:02d}/"
        f"{filename}"
    )


class BronzeWriter:
    """Writes raw events to the Bronze zone in MinIO."""

    def __init__(self, client: Optional[MinIOClient] = None) -> None:
        self._client = client or MinIOClient()
        self._client.ensure_bucket(BUCKET)

    def write_json_event(
        self,
        source: str,
        event: dict,
        event_id: str,
        ts: Optional[datetime] = None,
    ) -> str:
        """
        Write a single JSON event to Bronze.

        Args:
            source:   Source identifier (e.g., 'stock-ticks', 'trade-events')
            event:    The raw event dict (will be JSON-serialized)
            event_id: Unique ID for the event (used as filename)
            ts:       Timestamp for partitioning (defaults to now UTC)

        Returns:
            The MinIO object path where the event was written.
        """
        ts = ts or datetime.now(timezone.utc)
        path = _build_path(source=source, ts=ts, filename=f"{event_id}.json")
        data = json.dumps(event, default=str).encode("utf-8")
        self._client.put_object(
            object_path=path,
            data=data,
            content_type="application/json",
        )
        return f"s3a://{BUCKET}/{path}"

    def write_json_batch(
        self,
        source: str,
        events: list[dict],
        batch_id: str,
        ts: Optional[datetime] = None,
    ) -> str:
        """
        Write a batch of JSON events as a single newline-delimited JSON file.

        Returns:
            The MinIO object path.
        """
        ts = ts or datetime.now(timezone.utc)
        path = _build_path(source=source, ts=ts, filename=f"{batch_id}.ndjson")
        ndjson = "\n".join(json.dumps(e, default=str) for e in events)
        data = ndjson.encode("utf-8")
        self._client.put_object(
            object_path=path,
            data=data,
            content_type="application/x-ndjson",
        )
        logger.info(
            f"Bronze batch written: {len(events)} events → "
            f"s3a://{BUCKET}/{path}"
        )
        return f"s3a://{BUCKET}/{path}"

    def write_raw_bytes(
        self,
        source: str,
        data: bytes,
        filename: str,
        ts: Optional[datetime] = None,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write arbitrary bytes to Bronze (e.g., CSV from batch ingestion)."""
        ts = ts or datetime.now(timezone.utc)
        path = _build_path(source=source, ts=ts, filename=filename)
        self._client.put_object(
            object_path=path,
            data=data,
            content_type=content_type,
        )
        return f"s3a://{BUCKET}/{path}"

    def list_partition(
        self,
        source: str,
        year: int,
        month: int,
        day: int,
        hour: Optional[int] = None,
    ) -> list[dict]:
        """List all objects in a Bronze partition."""
        prefix = (
            f"{BRONZE_PREFIX}/{source}/"
            f"year={year}/month={month:02d}/day={day:02d}/"
        )
        if hour is not None:
            prefix += f"hour={hour:02d}/"
        return self._client.list_objects(prefix=prefix)
