"""
test_ranger_client.py — Unit tests for Ranger RBAC policy engine.
"""
import pytest
import sys

sys.path.insert(0, "src")

from security.ranger_client import RangerPolicyEngine


class TestRangerPolicyEngine:
    def setup_method(self):
        self.engine = RangerPolicyEngine()

    # ── Role-based access checks ───────────────────────────────────────────────

    def test_admin_has_all_permissions(self):
        assert self.engine.check_access("admin", "read_bronze") is True
        assert self.engine.check_access("admin", "write_gold") is True
        assert self.engine.check_access("admin", "anything_at_all") is True

    def test_analyst_can_read_silver_and_gold(self):
        assert self.engine.check_access("analyst", "read_silver") is True
        assert self.engine.check_access("analyst", "read_gold") is True

    def test_analyst_cannot_write(self):
        assert self.engine.check_access("analyst", "write_silver") is False
        assert self.engine.check_access("analyst", "submit_spark") is False

    def test_analyst_cannot_read_bronze(self):
        assert self.engine.check_access("analyst", "read_bronze") is False

    def test_engineer_can_read_write_all_zones(self):
        for zone in ["bronze", "silver", "gold"]:
            assert self.engine.check_access("engineer", f"read_{zone}") is True
            assert self.engine.check_access("engineer", f"write_{zone}") is True

    def test_unknown_role_denied(self):
        assert self.engine.check_access("hacker", "read_silver") is False
        assert self.engine.check_access("guest", "read_gold") is False

    def test_api_consumer_can_read_gold_only(self):
        assert self.engine.check_access("api_consumer", "read_gold") is True
        assert self.engine.check_access("api_consumer", "read_silver") is False

    # ── Column masking ─────────────────────────────────────────────────────────

    def test_admin_sees_real_email(self):
        result = self.engine.get_masked_value("email", "alice@example.com", "admin")
        assert result == "alice@example.com"

    def test_analyst_email_masked(self):
        result = self.engine.get_masked_value("email", "alice@example.com", "analyst")
        assert result == "alic***"

    def test_full_name_masked_for_analyst(self):
        result = self.engine.get_masked_value("full_name", "Alice Smith", "analyst")
        assert result == "***MASKED***"

    def test_non_pii_column_unmasked_for_analyst(self):
        result = self.engine.get_masked_value("ticker", "AAPL", "analyst")
        assert result == "AAPL"

    # ── Row filtering ──────────────────────────────────────────────────────────

    def test_admin_sees_all_rows(self):
        records = [{"country": "US"}, {"country": "GB"}, {"country": "DE"}]
        result = self.engine.apply_row_filter(records, "admin")
        assert len(result) == 3

    def test_analyst_filtered_by_country(self):
        records = [{"country": "US"}, {"country": "GB"}, {"country": "US"}]
        result = self.engine.apply_row_filter(
            records, "analyst", filter_column="country", filter_value="US"
        )
        assert len(result) == 2
        assert all(r["country"] == "US" for r in result)
