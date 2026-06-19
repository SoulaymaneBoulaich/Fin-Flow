"""
silver.py — Silver zone: cleaned, typed, deduplicated data in Parquet.

This module provides:
 - Read from Bronze zone (raw JSON)
 - Write cleaned Parquet to Silver zone
 - Deduplication tracking via hash set
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from storage.minio.client import MinIOClient


SILVER_PREFIX = os.getenv("MINIO_SILVER_PREFIX", "silver")
BUCKET = os.getenv("MINIO_BUCKET", "finflow")


def _build_silver_path(source: str, ticker: str, ts: datetime) -> str:
    """
    Silver path pattern: silver/{source}/ticker={T}/year={Y}/month={MM}/day={DD}/
    """
    return (
        f"{SILVER_PREFIX}/{source}/"
        f"ticker={ticker.upper()}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
    )


def compute_dedup_hash(row: dict, key_fields: list[str]) -> str:
    """Compute a deterministic deduplication hash over selected fields."""
    key = "|".join(str(row.get(f, "")) for f in key_fields)
    return hashlib.sha256(key.encode()).hexdigest()


class SilverWriter:
    """Writes cleaned data as Parquet to the Silver zone."""

    def __init__(self, client: Optional[MinIOClient] = None) -> None:
        self._client = client or MinIOClient()
        self._client.ensure_bucket(BUCKET)

    def write_tick_parquet(
        self,
        ticks: list[dict],
        ticker: str,
        batch_id: str,
        ts: Optional[datetime] = None,
        dedup_key_fields: Optional[list[str]] = None,
    ) -> str:
        """
        Write a list of cleaned tick dicts to Silver as Parquet.

        Steps applied here:
         1. Type casting (prices as float, volume as int, timestamp as datetime)
         2. Deduplication via hash
         3. Range validation (price > 0, volume >= 0)
         4. Parquet serialization

        Returns the S3A path written.
        """
        ts = ts or datetime.now(timezone.utc)
        dedup_keys = dedup_key_fields or ["ticker", "timestamp", "close"]

        seen_hashes: set[str] = set()
        cleaned: list[dict] = []

        for row in ticks:
            # ── Type casting ──────────────────────────────────────────────
            try:
                row["close"] = float(row.get("close", 0))
                row["open"] = float(row.get("open", row["close"]))
                row["high"] = float(row.get("high", row["close"]))
                row["low"] = float(row.get("low", row["close"]))
                row["volume"] = int(row.get("volume", 0))
                row["vwap"] = float(row["vwap"]) if row.get("vwap") else row["close"]
            except (ValueError, TypeError) as exc:
                logger.warning(f"Type cast failed for row: {exc}. Skipping.")
                continue

            # ── Range validation ──────────────────────────────────────────
            if row["close"] <= 0 or row["volume"] < 0:
                logger.warning(f"Invalid range for row: close={row['close']}, vol={row['volume']}. Skipping.")
                continue

            # ── Deduplication ─────────────────────────────────────────────
            dh = compute_dedup_hash(row, dedup_keys)
            if dh in seen_hashes:
                continue
            seen_hashes.add(dh)
            row["_dedup_hash"] = dh

            cleaned.append(row)

        if not cleaned:
            logger.warning(f"No valid records for Silver write (ticker={ticker})")
            return ""

        # ── Build PyArrow table ───────────────────────────────────────────
        schema = pa.schema([
            pa.field("event_id", pa.string()),
            pa.field("ticker", pa.string()),
            pa.field("timestamp", pa.string()),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("volume", pa.int64()),
            pa.field("vwap", pa.float64()),
            pa.field("source", pa.string()),
            pa.field("_dedup_hash", pa.string()),
        ])

        table = pa.Table.from_pylist(cleaned, schema=schema)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        parquet_bytes = buf.read()

        # ── Write to Silver ───────────────────────────────────────────────
        prefix = _build_silver_path(source="stock-ticks", ticker=ticker, ts=ts)
        object_path = f"{prefix}{batch_id}.parquet"

        self._client.put_object(
            object_path=object_path,
            data=parquet_bytes,
            content_type="application/octet-stream",
        )

        logger.info(
            f"Silver: {len(cleaned)} records written → "
            f"s3a://{BUCKET}/{object_path}"
        )
        return f"s3a://{BUCKET}/{object_path}"

    def read_parquet(self, object_path: str) -> pa.Table:
        """Read a Parquet file from MinIO into a PyArrow table."""
        raw = self._client.get_object(object_path)
        return pq.read_table(io.BytesIO(raw))
