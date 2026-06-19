"""
ranger_client.py — Apache Ranger policy enforcement client.

Simulates Ranger RBAC checks locally (real implementation uses Ranger REST API).
In production: replace _check_ranger_api() with actual Ranger plugin.
"""
from __future__ import annotations

import json
import os
from typing import Optional

import requests
from loguru import logger


# Role → Permission mapping (mirrors config/ranger/policies/finflow_policies.json)
_ROLE_PERMISSIONS: dict[str, set[str]] = {
    "analyst": {
        "read_silver", "read_gold", "access_superset",
    },
    "engineer": {
        "read_bronze", "write_bronze", "read_silver", "write_silver",
        "read_gold", "write_gold", "submit_spark", "manage_airflow",
    },
    "admin": {"*"},
    "api_consumer": {"read_gold", "read_api"},
}

# Columns that are masked for non-admin roles
_MASKED_COLUMNS: dict[str, str] = {
    "email": "MASK_SHOW_FIRST_4",
    "full_name": "MASK",
    "date_of_birth": "MASK",
}


class RangerPolicyEngine:
    """
    Local Ranger policy engine for access control decisions.
    """

    def __init__(self, ranger_url: Optional[str] = None) -> None:
        self._ranger_url = ranger_url or os.getenv("RANGER_URL")

    def check_access(
        self,
        user_role: str,
        permission: str,
    ) -> bool:
        """
        Check if a role has a specific permission.

        Returns True if access is granted.
        """
        role_perms = _ROLE_PERMISSIONS.get(user_role, set())
        if "*" in role_perms:
            return True
        granted = permission in role_perms
        if not granted:
            logger.warning(
                f"ACCESS DENIED: role='{user_role}' requested permission='{permission}'"
            )
        return granted

    def get_masked_value(
        self,
        column_name: str,
        value: str,
        user_role: str,
    ) -> str:
        """
        Apply column-level masking based on Ranger policy.
        Admin role sees real values; others see masked values.
        """
        if user_role == "admin":
            return value

        mask_type = _MASKED_COLUMNS.get(column_name.lower())
        if not mask_type:
            return value

        if mask_type == "MASK_SHOW_FIRST_4":
            return value[:4] + "***" if len(value) >= 4 else "***"
        elif mask_type == "MASK":
            return "***MASKED***"
        return value

    def apply_row_filter(
        self,
        records: list[dict],
        user_role: str,
        filter_column: Optional[str] = None,
        filter_value: Optional[str] = None,
    ) -> list[dict]:
        """
        Apply row-level filtering based on role and filter conditions.
        Example: analysts only see US data.
        """
        if user_role == "admin":
            return records

        if filter_column and filter_value:
            filtered = [r for r in records if r.get(filter_column) == filter_value]
            logger.debug(
                f"Row filter applied: {len(records)} → {len(filtered)} records "
                f"({filter_column}={filter_value})"
            )
            return filtered

        return records

    def audit_log(
        self,
        user: str,
        role: str,
        action: str,
        resource: str,
        granted: bool,
    ) -> None:
        """Log an access control decision to the audit trail."""
        entry = {
            "user": user,
            "role": role,
            "action": action,
            "resource": resource,
            "granted": granted,
            "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
        }
        logger.info(f"[RANGER AUDIT] {entry}")
