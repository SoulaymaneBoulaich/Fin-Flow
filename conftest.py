"""
conftest.py — Shared pytest fixtures and configuration.
"""
import sys
import os
import pytest

# Add src to the Python path for all tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


@pytest.fixture
def fake_minio_client():
    """Provide a fake in-memory MinIO client for unit tests."""

    class _FakeMinIOClient:
        def __init__(self):
            self._store = {}
            self._buckets = set()

        def ensure_bucket(self, bucket):
            self._buckets.add(bucket)

        def put_object(self, object_path, data, **kwargs):
            self._store[object_path] = data

        def get_object(self, object_path, **kwargs):
            return self._store.get(object_path, b"")

        def list_objects(self, prefix="", **kwargs):
            return [{"name": k, "size": len(v)} for k, v in self._store.items()
                    if k.startswith(prefix)]

        def object_exists(self, object_path, **kwargs):
            return object_path in self._store

        def is_healthy(self):
            return True

    return _FakeMinIOClient()


@pytest.fixture
def sample_ticks():
    """Return a list of sample tick event dicts."""
    return [
        {
            "event_id": f"evt-{i:04d}",
            "ticker": "AAPL",
            "timestamp": "2024-06-15T14:00:00",
            "open": 184.0 + i * 0.1,
            "high": 185.5,
            "low": 183.5,
            "close": 185.0 + i * 0.05,
            "volume": 100_000 + i * 1000,
            "vwap": 184.8,
            "source": "yfinance",
        }
        for i in range(10)
    ]
