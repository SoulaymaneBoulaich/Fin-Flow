"""
test_pii_engine.py — Unit tests for PII detection and masking.
"""
import pytest
import sys

sys.path.insert(0, "src")

from privacy.pii_engine import PIIDetector, PIIMasker


class TestPIIDetector:
    def setup_method(self):
        self.detector = PIIDetector()

    def test_detects_email(self):
        assert self.detector.detect_pii_in_value("user@example.com") is True

    def test_detects_ssn(self):
        assert self.detector.detect_pii_in_value("123-45-6789") is True

    def test_detects_ip(self):
        assert self.detector.detect_pii_in_value("192.168.1.100") is True

    def test_safe_value_not_detected(self):
        assert self.detector.detect_pii_in_value("AAPL") is False
        assert self.detector.detect_pii_in_value("172.65") is False  # price
        assert self.detector.detect_pii_in_value("1000000") is False

    def test_column_name_heuristic_email(self):
        assert self.detector.detect_pii_column("email") is True
        assert self.detector.detect_pii_column("email_address") is True

    def test_column_name_heuristic_safe(self):
        assert self.detector.detect_pii_column("ticker") is False
        assert self.detector.detect_pii_column("close") is False
        assert self.detector.detect_pii_column("volume") is False

    def test_scan_record(self):
        record = {
            "ticker": "AAPL",
            "close": "185.0",
            "email": "john@test.com",
            "full_name": "John Smith",
        }
        findings = self.detector.scan_record(record)
        assert "email" in findings
        assert "full_name" in findings
        assert "ticker" not in findings
        assert "close" not in findings


class TestPIIMasker:
    def setup_method(self):
        self.masker = PIIMasker(hmac_secret="test_secret", aes_key="test_key_32_chars_minimum!!")

    def test_mask_email(self):
        masked = self.masker.mask_email("john.smith@gmail.com")
        assert "***" in masked
        assert "j" in masked       # First letter preserved
        assert "gmail.com" not in masked

    def test_mask_name(self):
        masked = self.masker.mask_name("John Smith")
        assert "***" in masked
        # First letters preserved
        assert "J" in masked
        assert "S" in masked

    def test_mask_phone(self):
        masked = self.masker.mask_phone("+447911123456")
        assert "3456" in masked    # Last 4 digits preserved
        assert "*" in masked

    def test_pseudonymize_deterministic(self):
        """Same input + same key → same pseudonym (required for joins)."""
        p1 = self.masker.pseudonymize("john@example.com")
        p2 = self.masker.pseudonymize("john@example.com")
        assert p1 == p2
        assert len(p1) == 64  # SHA-256 hex digest

    def test_pseudonymize_different_inputs(self):
        """Different inputs → different pseudonyms."""
        p1 = self.masker.pseudonymize("alice@example.com")
        p2 = self.masker.pseudonymize("bob@example.com")
        assert p1 != p2

    def test_generalize_age(self):
        assert self.masker.generalize_age(34) == "30-39"
        assert self.masker.generalize_age(25) == "20-29"
        assert self.masker.generalize_age(40) == "40-49"

    def test_generalize_dob(self):
        assert self.masker.generalize_dob("1988-04-22") == "1980s"
        assert self.masker.generalize_dob("1995-12-01") == "1990s"

    def test_apply_to_record_pseudonymize(self):
        record = {
            "email": "alice@example.com",
            "ticker": "AAPL",
            "close": 185.0,
        }
        result = self.masker.apply_to_record(record, strategy="pseudonymize")
        assert result["ticker"] == "AAPL"   # Non-PII unchanged
        assert result["close"] == 185.0     # Non-PII unchanged
        assert result["email"] != "alice@example.com"  # PII transformed
        assert len(result["email"]) == 64   # Pseudonym

    def test_apply_to_record_mask(self):
        record = {"email": "alice@example.com"}
        result = self.masker.apply_to_record(record, strategy="mask")
        assert "***" in result["email"]
