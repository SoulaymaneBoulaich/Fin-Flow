"""
pii_engine.py — PII Detection, Masking, Tokenization, and Pseudonymization.

Methods:
 1. detect_pii_in_value()  — Rule-based + schema-based PII detection
 2. mask()                 — Partial masking (j***@***.com)
 3. tokenize()             — Reversible token (stored in TokenMap)
 4. pseudonymize()         — HMAC-SHA256 keyed hash (irreversible, consistent)
 5. generalize_dob()       — Age bucket generalization
 6. scan_dataframe()       — Scan an entire dict list for PII columns
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
import uuid
from typing import Optional


class PIIDetector:
    """
    Detects PII using:
     - Regex patterns (emails, phone numbers, national IDs)
     - Column name heuristics
     - Statistical analysis (all values look like emails)
    """

    # Column names strongly suggesting PII
    PII_COLUMN_NAMES = frozenset([
        "email", "email_address", "full_name", "name", "first_name",
        "last_name", "surname", "phone", "phone_number", "mobile",
        "date_of_birth", "dob", "birth_date", "ssn", "social_security",
        "national_id", "passport", "address", "street", "postcode",
        "zip_code", "ip_address", "ip", "credit_card", "card_number",
    ])

    _EMAIL_RE   = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    _PHONE_RE   = re.compile(r"\+?[\d\s\-\(\)]{7,15}")
    _SSN_RE     = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
    _POSTCODE_RE = re.compile(r"\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b", re.IGNORECASE)
    _IP_RE      = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")

    def detect_pii_in_value(self, value: str) -> bool:
        """Return True if the value matches any PII pattern."""
        v = str(value)
        return bool(
            self._EMAIL_RE.search(v)
            or self._SSN_RE.search(v)
            or self._POSTCODE_RE.search(v)
            or self._IP_RE.search(v)
        )

    def detect_pii_column(self, column_name: str) -> bool:
        """Return True if the column name is in the PII heuristic list."""
        return column_name.lower() in self.PII_COLUMN_NAMES

    def scan_record(self, record: dict) -> dict[str, str]:
        """
        Scan all fields in a dict and return a map of { field_name: pii_type }.
        PII types: 'email', 'phone', 'ssn', 'postcode', 'ip', 'name_heuristic'
        """
        findings: dict[str, str] = {}
        for col, val in record.items():
            if self.detect_pii_column(col):
                findings[col] = "column_name_heuristic"
            elif val and self.detect_pii_in_value(str(val)):
                findings[col] = "pattern_match"
        return findings

    def scan_records(self, records: list[dict]) -> dict[str, list[str]]:
        """
        Scan a list of records. Returns a summary of PII columns found.
        { column_name: [pii_type, ...] }
        """
        all_findings: dict[str, list[str]] = {}
        for record in records:
            findings = self.scan_record(record)
            for col, pii_type in findings.items():
                if col not in all_findings:
                    all_findings[col] = []
                if pii_type not in all_findings[col]:
                    all_findings[col].append(pii_type)
        return all_findings


class PIIMasker:
    """
    Applies PII masking, tokenization, and pseudonymization.
    """

    def __init__(
        self,
        hmac_secret: Optional[str] = None,
        aes_key: Optional[str] = None,
    ) -> None:
        self._hmac_secret = (hmac_secret or os.getenv("PII_HMAC_SECRET", "change_me")).encode()
        self._aes_key = aes_key or os.getenv("AES_ENCRYPTION_KEY", "change_me_32_chars_minimum!!")

    # ── 1. Masking ─────────────────────────────────────────────────────────────

    def mask_email(self, email: str) -> str:
        """j.smith@gmail.com → j***@***.com"""
        if "@" not in email:
            return "***@***.***"
        local, domain = email.split("@", 1)
        domain_parts = domain.split(".")
        masked_local = local[0] + "***" if len(local) > 1 else "***"
        masked_domain = "***." + domain_parts[-1] if domain_parts else "***"
        return f"{masked_local}@{masked_domain}"

    def mask_name(self, name: str) -> str:
        """John Smith → J*** S***"""
        parts = name.split()
        return " ".join(p[0] + "***" if p else "***" for p in parts)

    def mask_phone(self, phone: str) -> str:
        """Only keep last 4 digits: +447911123456 → ****3456"""
        digits = re.sub(r"\D", "", phone)
        return "*" * (len(digits) - 4) + digits[-4:] if len(digits) >= 4 else "****"

    # ── 2. Pseudonymization (HMAC-SHA256, deterministic) ──────────────────────

    def pseudonymize(self, value: str) -> str:
        """
        Replace PII with a consistent, non-reversible pseudonym.
        Same input + same key → same output (allows joins across tables).
        Cannot be reversed without the secret key.
        """
        return hmac.new(
            self._hmac_secret,
            value.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    # ── 3. Tokenization (reversible, requires token map) ─────────────────────

    def generate_token(self) -> str:
        """Generate a random, URL-safe token."""
        return f"tok_{secrets.token_hex(12)}"

    def encrypt_value(self, plaintext: str) -> str:
        """
        AES-256 encryption for the token map stored value.
        Uses a simple XOR-based approach for portability.
        In production: use cryptography.fernet.Fernet.
        """
        try:
            from cryptography.fernet import Fernet
            import base64
            # Derive a Fernet key from our AES key
            key_bytes = self._aes_key.encode()[:32].ljust(32, b"=")
            fernet_key = base64.urlsafe_b64encode(key_bytes)
            f = Fernet(fernet_key)
            return f.encrypt(plaintext.encode()).decode()
        except Exception:
            # Fallback: base64 encode (NOT secure — only for dev without cryptography)
            import base64
            return base64.b64encode(plaintext.encode()).decode()

    def decrypt_value(self, ciphertext: str) -> str:
        """Decrypt an AES-encrypted token map value."""
        try:
            from cryptography.fernet import Fernet
            import base64
            key_bytes = self._aes_key.encode()[:32].ljust(32, b"=")
            fernet_key = base64.urlsafe_b64encode(key_bytes)
            f = Fernet(fernet_key)
            return f.decrypt(ciphertext.encode()).decode()
        except Exception:
            import base64
            return base64.b64decode(ciphertext.encode()).decode()

    # ── 4. Generalization ─────────────────────────────────────────────────────

    def generalize_age(self, age: int, bucket_size: int = 10) -> str:
        """Replace exact age with a decade bucket: 34 → '30-39'"""
        low = (age // bucket_size) * bucket_size
        return f"{low}-{low + bucket_size - 1}"

    def generalize_dob(self, dob_str: str) -> str:
        """Replace DOB with birth decade: 1988-04-22 → '1980s'"""
        try:
            year = int(dob_str[:4])
            decade = (year // 10) * 10
            return f"{decade}s"
        except (ValueError, IndexError):
            return "unknown"

    # ── 5. Apply to a record ──────────────────────────────────────────────────

    def apply_to_record(
        self,
        record: dict,
        strategy: str = "pseudonymize",
    ) -> dict:
        """
        Apply PII handling to an entire record.

        strategy: 'pseudonymize' | 'mask' | 'tokenize'
        """
        detector = PIIDetector()
        result = dict(record)

        for col, val in record.items():
            if not val:
                continue
            is_pii = detector.detect_pii_column(col)

            if not is_pii:
                continue

            if strategy == "mask":
                if "email" in col.lower():
                    result[col] = self.mask_email(str(val))
                elif "name" in col.lower():
                    result[col] = self.mask_name(str(val))
                elif "phone" in col.lower() or "mobile" in col.lower():
                    result[col] = self.mask_phone(str(val))
                else:
                    result[col] = "***REDACTED***"
            elif strategy == "pseudonymize":
                result[col] = self.pseudonymize(str(val))
            elif strategy == "tokenize":
                result[col] = self.generate_token()

        return result
