"""
tick_store.py — High-performance tick data read/write via Cassandra.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from cassandra.query import SimpleStatement, ConsistencyLevel
from loguru import logger

from storage.cassandra.client import CassandraClient


class TickStore:
    """
    Cassandra-backed tick data store.

    Write path: single tick insert + batch insert
    Read path:  latest N ticks, range query, all tickers summary
    """

    # Prepared statements are cached here after first call
    _insert_stmt = None
    _latest_stmt = None

    def __init__(self, client: Optional[CassandraClient] = None) -> None:
        self._client = client or CassandraClient()
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse (avoids repeated parsing)."""
        self._insert_stmt = self._client.session.prepare(
            """
            INSERT INTO tick_data
              (ticker, timestamp, event_id, open, high, low, close,
               volume, vwap, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self._insert_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        self._latest_stmt = self._client.session.prepare(
            """
            SELECT * FROM tick_data
            WHERE ticker = ?
            LIMIT ?
            """
        )

    def insert_tick(
        self,
        ticker: str,
        timestamp: datetime,
        event_id: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        vwap: float,
        source: str = "yfinance",
    ) -> None:
        """Insert a single tick event."""
        now = datetime.now(timezone.utc)
        self._client.session.execute(
            self._insert_stmt,
            (ticker, timestamp, event_id, open_, high, low, close,
             volume, vwap, source, now),
        )

    def insert_ticks_batch(self, ticks: list[dict]) -> int:
        """
        Insert multiple ticks in parallel using async futures.
        Returns the number of successfully written ticks.
        """
        futures = []
        for tick in ticks:
            future = self._client.session.execute_async(
                self._insert_stmt,
                (
                    tick["ticker"],
                    tick["timestamp"],
                    tick.get("event_id", ""),
                    float(tick["open"]),
                    float(tick["high"]),
                    float(tick["low"]),
                    float(tick["close"]),
                    int(tick["volume"]),
                    float(tick.get("vwap", tick["close"])),
                    tick.get("source", "yfinance"),
                    datetime.now(timezone.utc),
                ),
            )
            futures.append(future)

        # Wait for all futures and count successes
        success = 0
        for future in futures:
            try:
                future.result()
                success += 1
            except Exception as exc:
                logger.error(f"Cassandra async insert failed: {exc}")

        logger.debug(f"Cassandra batch: {success}/{len(ticks)} ticks written")
        return success

    def get_latest_ticks(self, ticker: str, limit: int = 100) -> list[dict]:
        """Return the most recent N ticks for a ticker (newest first)."""
        rows = self._client.session.execute(self._latest_stmt, (ticker, limit))
        return [dict(row._asdict()) for row in rows]

    def get_ticks_in_range(
        self,
        ticker: str,
        start: datetime,
        end: datetime,
    ) -> list[dict]:
        """Return all ticks for a ticker between start and end timestamps."""
        stmt = SimpleStatement(
            """
            SELECT * FROM tick_data
            WHERE ticker = %s
              AND timestamp >= %s
              AND timestamp <= %s
            """,
            consistency_level=ConsistencyLevel.LOCAL_ONE,
        )
        rows = self._client.session.execute(stmt, (ticker, start, end))
        return [dict(row._asdict()) for row in rows]

    def get_latest_price(self, ticker: str) -> Optional[float]:
        """Return the most recent close price for a ticker."""
        ticks = self.get_latest_ticks(ticker, limit=1)
        return ticks[0]["close"] if ticks else None
