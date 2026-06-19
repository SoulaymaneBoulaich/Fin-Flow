"""
client.py — Cassandra connection pool and session management.
"""
from __future__ import annotations

import os
from typing import Optional

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.auth import PlainTextAuthProvider
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential


_DEFAULT_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "finflow")


class CassandraClient:
    """Thread-safe Cassandra session manager."""

    def __init__(
        self,
        hosts: Optional[list[str]] = None,
        port: int = 9042,
        keyspace: str = _DEFAULT_KEYSPACE,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self._hosts = hosts or [os.getenv("CASSANDRA_HOST", "cassandra")]
        self._port = port
        self._keyspace = keyspace
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None
        self._username = username
        self._password = password

    @retry(stop=stop_after_attempt(10), wait=wait_exponential(min=2, max=30))
    def connect(self) -> Session:
        """Connect to Cassandra cluster with retry (Cassandra can be slow to start)."""
        auth = None
        if self._username and self._password:
            auth = PlainTextAuthProvider(
                username=self._username, password=self._password
            )

        self._cluster = Cluster(
            contact_points=self._hosts,
            port=self._port,
            auth_provider=auth,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
            default_retry_policy=RetryPolicy(),
            protocol_version=4,
        )
        self._session = self._cluster.connect()

        # Ensure keyspace exists
        self._session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self._keyspace}
            WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        self._session.set_keyspace(self._keyspace)
        logger.info(
            f"Cassandra connected: hosts={self._hosts} keyspace={self._keyspace}"
        )
        return self._session

    @property
    def session(self) -> Session:
        if self._session is None:
            self.connect()
        return self._session

    def execute(self, query: str, parameters=None):
        """Execute a CQL statement."""
        return self.session.execute(query, parameters)

    def is_healthy(self) -> bool:
        """Quick health check."""
        try:
            self.session.execute("SELECT now() FROM system.local")
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Shut down the cluster connection."""
        if self._cluster:
            self._cluster.shutdown()
            logger.info("Cassandra connection closed.")

    def __enter__(self) -> "CassandraClient":
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()
