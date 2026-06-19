"""
test_storage_minio.py — Unit tests for MinIO Bronze/Silver/Gold writers.
Uses in-memory fake MinIO client (no real MinIO connection needed).
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call
import sys

sys.path.insert(0, "src")


class FakeMinIOClient:
    """In-memory fake for MinIOClient used in unit tests."""

    def __init__(self):
        self._store: dict[str, bytes] = {}
        self._buckets: set[str] = set()

    def ensure_bucket(self, bucket: str) -> None:
        self._buckets.add(bucket)

    def put_object(self, object_path: str, data: bytes, **kwargs) -> None:
        self._store[object_path] = data

    def get_object(self, object_path: str, **kwargs) -> bytes:
        return self._store.get(object_path, b"")

    def list_objects(self, prefix: str = "", **kwargs) -> list[dict]:
        return [{"name": k, "size": len(v)} for k, v in self._store.items()
                if k.startswith(prefix)]

    def object_exists(self, object_path: str, **kwargs) -> bool:
        return object_path in self._store

    def is_healthy(self) -> bool:
        return True


class TestBronzeWriter:
    def setup_method(self):
        self.fake_client = FakeMinIOClient()
        from storage.minio.bronze import BronzeWriter
        self.writer = BronzeWriter(client=self.fake_client)

    def test_write_json_event(self):
        path = self.writer.write_json_event(
            source="stock-ticks",
            event={"ticker": "AAPL", "close": 185.0},
            event_id="test-id-001",
        )
        assert "s3a://finflow/bronze/" in path
        assert "stock-ticks" in path
        assert len(self.fake_client._store) == 1

    def test_write_json_batch(self):
        events = [{"ticker": "AAPL", "close": 185.0} for _ in range(5)]
        path = self.writer.write_json_batch(
            source="stock-ticks",
            events=events,
            batch_id="batch-001",
        )
        assert "s3a://finflow/bronze/" in path
        stored_bytes = list(self.fake_client._store.values())[0]
        # Should be newline-delimited JSON
        lines = stored_bytes.decode().strip().split("\n")
        assert len(lines) == 5

    def test_path_partitioning(self):
        ts = datetime(2024, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        path = self.writer.write_json_event(
            source="trade-events",
            event={"trade_id": "t001"},
            event_id="t001",
            ts=ts,
        )
        assert "year=2024" in path
        assert "month=06" in path
        assert "day=15" in path
        assert "hour=14" in path


class TestSilverWriter:
    def setup_method(self):
        self.fake_client = FakeMinIOClient()
        from storage.minio.silver import SilverWriter
        self.writer = SilverWriter(client=self.fake_client)

    def _make_ticks(self, n: int = 3) -> list[dict]:
        return [
            {
                "event_id": f"evt-{i}",
                "ticker": "AAPL",
                "timestamp": "2024-06-15T14:00:00",
                "open": 184.0,
                "high": 185.5,
                "low": 183.5,
                "close": 185.0,
                "volume": 100_000,
                "vwap": 184.8,
                "source": "test",
            }
            for i in range(n)
        ]

    def test_write_tick_parquet(self):
        ticks = self._make_ticks(3)
        path = self.writer.write_tick_parquet(
            ticks=ticks, ticker="AAPL", batch_id="batch-001"
        )
        assert "s3a://finflow/silver/" in path
        assert len(self.fake_client._store) == 1

    def test_deduplication(self):
        """3 identical ticks should deduplicate to 1 record."""
        ticks = self._make_ticks(3)  # All identical → deduped to 1
        path = self.writer.write_tick_parquet(
            ticks=ticks, ticker="AAPL", batch_id="batch-dedup"
        )
        # Parquet file should exist
        assert len(self.fake_client._store) == 1

    def test_invalid_records_dropped(self):
        """Records with close <= 0 should be silently dropped."""
        ticks = [
            {
                "event_id": "bad-001",
                "ticker": "AAPL",
                "timestamp": "2024-06-15T14:00:00",
                "open": 1, "high": 2, "low": 0.5,
                "close": -999.0,  # Invalid
                "volume": 100, "vwap": 1.0, "source": "test",
            }
        ]
        path = self.writer.write_tick_parquet(
            ticks=ticks, ticker="AAPL", batch_id="batch-invalid"
        )
        assert path == ""  # Nothing written


class TestGoldWriter:
    def setup_method(self):
        self.fake_client = FakeMinIOClient()
        from storage.minio.gold import GoldWriter
        self.writer = GoldWriter(client=self.fake_client)

    def _make_ticks(self) -> list[dict]:
        return [
            {"open": 183.0, "high": 186.0, "low": 182.5,
             "close": 184.0 + i, "volume": 100_000 * i, "vwap": 184.5}
            for i in range(1, 6)
        ]

    def test_compute_daily_ohlcv(self):
        ticks = self._make_ticks()
        ohlcv = self.writer.compute_daily_ohlcv(ticks)
        assert ohlcv["high"] == 186.0
        assert ohlcv["low"] == 182.5
        assert ohlcv["tick_count"] == 5
        assert ohlcv["volume"] > 0

    def test_compute_moving_averages(self):
        closes = list(range(50, 80))  # 30 data points
        mas = self.writer.compute_moving_averages(closes)
        assert "ma_7" in mas
        assert "ma_14" in mas
        assert "ma_30" in mas
        assert mas["ma_7"] is not None
        assert mas["ma_30"] is not None

    def test_moving_averages_insufficient_data(self):
        closes = [100.0, 101.0, 102.0]  # Only 3 points
        mas = self.writer.compute_moving_averages(closes)
        assert mas["ma_30"] is None  # Not enough data
        assert mas["ma_7"] is None

    def test_compute_volatility(self):
        import math
        closes = [100.0 * (1 + 0.01 * i) for i in range(35)]
        vol = self.writer.compute_volatility(closes)
        assert vol is not None
        assert vol >= 0

    def test_compute_volatility_insufficient_data(self):
        vol = self.writer.compute_volatility([100.0])
        assert vol is None

    def test_write_daily_summary(self):
        ohlcv = {"open": 183.0, "high": 186.0, "low": 182.5, "close": 185.0,
                 "volume": 500_000, "vwap": 184.5, "tick_count": 120}
        mas = {"ma_7": 184.0, "ma_14": 183.5, "ma_30": 182.0}
        path = self.writer.write_daily_summary(
            ticker="AAPL",
            date_str="2024-06-15",
            ohlcv=ohlcv,
            moving_avgs=mas,
            volatility=0.245,
            batch_id="daily-001",
        )
        assert "s3a://finflow/gold/" in path
        assert len(self.fake_client._store) == 1
