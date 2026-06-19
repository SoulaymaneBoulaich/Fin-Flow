"""
client.py — PostgreSQL connection management using SQLAlchemy.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.postgres.models import Base


def _build_dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "finflow")
    password = os.getenv("POSTGRES_PASSWORD", "FinFlow_PG_2024!")
    db = os.getenv("POSTGRES_APP_DB", "finflow_app")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


class PostgresClient:
    """
    PostgreSQL client wrapping SQLAlchemy engine + session factory.
    """

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or _build_dsn()
        self._engine = create_engine(
            self._dsn,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,
        )
        self._Session = sessionmaker(bind=self._engine)

    @retry(stop=stop_after_attempt(10), wait=wait_exponential(min=2, max=30))
    def initialize(self) -> None:
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self._engine)
        logger.info("PostgreSQL schema initialized (tables created if not exists).")

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Context manager that yields a database session with auto-commit/rollback."""
        s = self._Session()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    def execute_raw(self, sql: str, params: dict = None):
        """Execute raw SQL (for analytics queries)."""
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall()

    def is_healthy(self) -> bool:
        """Verify the connection is alive."""
        try:
            self.execute_raw("SELECT 1")
            return True
        except Exception:
            return False


# ─── Module-level singleton ───────────────────────────────────────────────────
_default_client: Optional[PostgresClient] = None


def get_db() -> PostgresClient:
    """Return the module-level default client (lazy init)."""
    global _default_client
    if _default_client is None:
        _default_client = PostgresClient()
    return _default_client
