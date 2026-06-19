"""
models.py — SQLAlchemy ORM models for PostgreSQL operational database.

Tables:
 - User: master user data
 - TradeRecord: persisted trade executions
 - PipelineRun: Airflow-style job tracking
 - ConsentRecord: GDPR consent management
 - TokenMap: PII tokenization mapping (encrypted)
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, String, DateTime, Boolean, Float, Integer,
    Text, ForeignKey, Index, UniqueConstraint, func,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def _uuid() -> str:
    return str(uuid.uuid4())


class User(Base):
    __tablename__ = "users"

    user_id = Column(String(36), primary_key=True, default=_uuid)
    full_name = Column(String(255), nullable=False)          # PII
    email_token = Column(String(255), nullable=False, unique=True)  # Tokenized PII
    date_of_birth_masked = Column(String(20), nullable=True) # Generalized PII
    country = Column(String(2), nullable=False)
    account_type = Column(String(20), nullable=False)
    marketing_consent = Column(Boolean, default=False)
    analytics_consent = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    is_deleted = Column(Boolean, default=False)

    trades = relationship("TradeRecord", back_populates="user")
    consent = relationship("ConsentRecord", back_populates="user", uselist=False)

    __table_args__ = (
        Index("idx_users_country", "country"),
        Index("idx_users_account_type", "account_type"),
    )

    def __repr__(self) -> str:
        return f"<User id={self.user_id} type={self.account_type}>"


class TradeRecord(Base):
    __tablename__ = "trade_records"

    trade_id = Column(String(36), primary_key=True, default=_uuid)
    user_id = Column(String(36), ForeignKey("users.user_id"), nullable=False)
    ticker = Column(String(10), nullable=False)
    action = Column(String(4), nullable=False)     # BUY | SELL
    quantity = Column(Integer, nullable=False)
    price = Column(Float, nullable=False)
    total_value = Column(Float, nullable=False)
    status = Column(String(20), default="EXECUTED")
    executed_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=func.now())

    user = relationship("User", back_populates="trades")

    __table_args__ = (
        Index("idx_trades_ticker", "ticker"),
        Index("idx_trades_user_id", "user_id"),
        Index("idx_trades_executed_at", "executed_at"),
    )


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(String(36), primary_key=True, default=_uuid)
    pipeline_name = Column(String(100), nullable=False)
    dag_id = Column(String(100), nullable=True)
    run_date = Column(DateTime, nullable=False)
    status = Column(String(20), nullable=False)  # RUNNING | SUCCESS | FAILED
    records_in = Column(Integer, default=0)
    records_out = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=func.now())
    finished_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    __table_args__ = (
        Index("idx_pipeline_runs_name", "pipeline_name"),
        Index("idx_pipeline_runs_date", "run_date"),
    )


class ConsentRecord(Base):
    __tablename__ = "consent_records"

    consent_id = Column(String(36), primary_key=True, default=_uuid)
    user_id = Column(String(36), ForeignKey("users.user_id"), nullable=False, unique=True)
    marketing_consent = Column(Boolean, default=False)
    analytics_consent = Column(Boolean, default=False)
    consent_version = Column(String(10), nullable=False, default="v1.0")
    consent_timestamp = Column(DateTime, default=func.now())
    ip_address_hash = Column(String(64), nullable=True)  # HMAC hash of IP — not raw IP

    user = relationship("User", back_populates="consent")


class TokenMap(Base):
    """
    Maps PII tokens back to original values.
    This table is in a separate, restricted schema in production.
    """
    __tablename__ = "token_map"

    token_id = Column(String(36), primary_key=True, default=_uuid)
    token = Column(String(64), nullable=False, unique=True, index=True)
    field_type = Column(String(50), nullable=False)   # email | phone | name
    encrypted_value = Column(Text, nullable=False)    # AES-256 encrypted original value
    created_at = Column(DateTime, default=func.now())
    user_id = Column(String(36), ForeignKey("users.user_id"), nullable=True)


class DataQualityCheck(Base):
    """Stores results of automated data quality checks."""
    __tablename__ = "data_quality_checks"

    check_id = Column(String(36), primary_key=True, default=_uuid)
    check_name = Column(String(100), nullable=False)
    table_name = Column(String(100), nullable=False)
    zone = Column(String(10), nullable=False)  # bronze | silver | gold
    dimension = Column(String(30), nullable=False)  # completeness | accuracy | etc.
    passed = Column(Boolean, nullable=False)
    metric_value = Column(Float, nullable=True)
    expected_value = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)
    checked_at = Column(DateTime, default=func.now())
    run_id = Column(String(36), ForeignKey("pipeline_runs.run_id"), nullable=True)

    __table_args__ = (
        Index("idx_dq_table_zone", "table_name", "zone"),
    )
