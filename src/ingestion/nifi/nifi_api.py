"""
nifi_api.py — Apache NiFi REST API client for programmatic control.

Covers:
 - Start/stop processor groups
 - Upload and instantiate flow templates
 - Monitor queue depths and flow status
 - Trigger batch ingestion flows
"""
from __future__ import annotations

import os
import time
from typing import Optional, Any

import requests
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed


class NiFiAPIClient:
    """
    NiFi REST API v1 client.
    All operations are idempotent and retry-safe.
    """

    def __init__(
        self,
        host: str = "nifi",
        port: int = 8443,
        username: str = "admin",
        password: str = "admin",
        use_ssl: bool = False,
    ) -> None:
        scheme = "https" if use_ssl else "http"
        self.base_url = f"{scheme}://{host}:{port}/nifi-api"
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._username = username
        self._password = password
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        """Authenticate and retrieve bearer token."""
        resp = self._session.post(
            f"{self.base_url}/access/token",
            data={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.text.strip()

    def _ensure_auth(self) -> None:
        if not self._token:
            try:
                self._token = self._get_token()
                self._session.headers["Authorization"] = f"Bearer {self._token}"
                logger.debug("NiFi authentication successful.")
            except Exception as exc:
                logger.warning(f"NiFi auth failed (NiFi may not require auth): {exc}")

    def _get(self, path: str, **kwargs) -> dict:
        self._ensure_auth()
        resp = self._session.get(f"{self.base_url}{path}", timeout=15, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, body: dict, **kwargs) -> dict:
        self._ensure_auth()
        resp = self._session.put(
            f"{self.base_url}{path}", json=body, timeout=15, **kwargs
        )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: Optional[dict] = None, **kwargs) -> dict:
        self._ensure_auth()
        resp = self._session.post(
            f"{self.base_url}{path}",
            json=body or {},
            timeout=15,
            **kwargs,
        )
        resp.raise_for_status()
        return resp.json()

    # ─── Flow operations ──────────────────────────────────────────────────────

    def get_root_process_group_id(self) -> str:
        """Return the ID of the root process group."""
        data = self._get("/flow/about")
        return data.get("about", {}).get("id", "root")

    def list_process_groups(self, parent_id: str = "root") -> list[dict]:
        """List all process groups under a parent."""
        data = self._get(f"/flow/process-groups/{parent_id}")
        return data.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])

    def get_process_group_status(self, group_id: str) -> dict:
        """Return status summary for a process group."""
        return self._get(f"/flow/process-groups/{group_id}/status")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def start_process_group(self, group_id: str) -> None:
        """Start all processors in a process group."""
        self._put(
            f"/flow/process-groups/{group_id}",
            body={"id": group_id, "state": "RUNNING"},
        )
        logger.info(f"NiFi process group {group_id} started.")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def stop_process_group(self, group_id: str) -> None:
        """Stop all processors in a process group."""
        self._put(
            f"/flow/process-groups/{group_id}",
            body={"id": group_id, "state": "STOPPED"},
        )
        logger.info(f"NiFi process group {group_id} stopped.")

    def get_queue_depth(self, connection_id: str) -> dict:
        """Return the current queue depth (count + bytes) for a connection."""
        data = self._get(f"/connections/{connection_id}/status")
        stats = data.get("connectionStatus", {}).get("aggregateSnapshot", {})
        return {
            "queued_count": stats.get("flowFilesQueued", 0),
            "queued_bytes": stats.get("bytesQueued", 0),
        }

    def trigger_batch_ingest(self, group_id: str, wait_seconds: int = 5) -> None:
        """Start a batch ingestion flow and wait for it to stabilize."""
        self.start_process_group(group_id)
        logger.info(f"Waiting {wait_seconds}s for batch ingestion to run...")
        time.sleep(wait_seconds)
        status = self.get_process_group_status(group_id)
        logger.info(f"NiFi batch ingest status: {status}")

    def is_healthy(self) -> bool:
        """Check if NiFi is reachable and responding."""
        try:
            self._get("/flow/about")
            return True
        except Exception:
            return False

    def get_system_diagnostics(self) -> dict:
        """Return NiFi cluster system diagnostics."""
        return self._get("/system-diagnostics")
