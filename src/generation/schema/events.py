"""
Pydantic schema definitions for all FinFlow events.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Import guard — keep schemas importable without pydantic installed at the top
# ─────────────────────────────────────────────────────────────────────────────
try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:
    raise ImportError("pydantic>=2 is required. Run: pip install pydantic>=2")


class TickEvent(BaseModel):
    """Real-time stock price tick from yFinance."""

    ticker: str = Field(..., min_length=1, max_length=10, description="Stock ticker symbol")
    timestamp: datetime = Field(..., description="UTC timestamp of the tick")
    open: float = Field(..., gt=0, description="Opening price for the period")
    high: float = Field(..., gt=0, description="Period high price")
    low: float = Field(..., gt=0, description="Period low price")
    close: float = Field(..., gt=0, description="Closing price for the period")
    volume: int = Field(..., ge=0, description="Total volume traded")
    vwap: Optional[float] = Field(None, gt=0, description="Volume-weighted average price")
    source: str = Field(default="yfinance", description="Data source identifier")
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique event ID")

    @field_validator("high")
    @classmethod
    def high_gte_low(cls, v: float, info) -> float:
        data = info.data
        if "low" in data and v < data["low"]:
            raise ValueError(f"high ({v}) must be >= low ({data['low']})")
        return v

    @field_validator("ticker")
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        return v.upper().strip()

    def to_kafka_key(self) -> bytes:
        return self.ticker.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class UserEvent(BaseModel):
    """Synthetic user profile / account event."""

    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(..., description="CREATED | UPDATED | DELETED")
    full_name: str = Field(..., description="User full name — PII")
    email: str = Field(..., description="Email address — PII")
    date_of_birth: str = Field(..., description="ISO date string YYYY-MM-DD — PII")
    country: str = Field(..., min_length=2, max_length=2, description="ISO 3166-1 alpha-2")
    account_type: str = Field(..., description="BASIC | PREMIUM | PROFESSIONAL")
    marketing_consent: bool = Field(default=False)
    analytics_consent: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @field_validator("account_type")
    @classmethod
    def validate_account_type(cls, v: str) -> str:
        allowed = {"BASIC", "PREMIUM", "PROFESSIONAL"}
        if v.upper() not in allowed:
            raise ValueError(f"account_type must be one of {allowed}")
        return v.upper()

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        allowed = {"CREATED", "UPDATED", "DELETED"}
        if v.upper() not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return v.upper()

    def to_kafka_key(self) -> bytes:
        return self.user_id.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class TradeEvent(BaseModel):
    """Synthetic trade execution event."""

    trade_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = Field(..., description="Foreign key to UserEvent.user_id")
    ticker: str = Field(..., min_length=1, max_length=10)
    action: str = Field(..., description="BUY | SELL")
    quantity: int = Field(..., gt=0, description="Number of shares")
    price: float = Field(..., gt=0, description="Execution price per share")
    total_value: float = Field(..., gt=0, description="Total trade value = quantity * price")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: str = Field(default="EXECUTED", description="EXECUTED | PENDING | CANCELLED")

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        allowed = {"BUY", "SELL"}
        if v.upper() not in allowed:
            raise ValueError(f"action must be one of {allowed}")
        return v.upper()

    @field_validator("ticker")
    @classmethod
    def ticker_uppercase(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("total_value")
    @classmethod
    def validate_total(cls, v: float, info) -> float:
        data = info.data
        if "quantity" in data and "price" in data:
            expected = round(data["quantity"] * data["price"], 4)
            if abs(v - expected) > 0.01:
                raise ValueError(
                    f"total_value {v} does not match quantity*price = {expected}"
                )
        return v

    def to_kafka_key(self) -> bytes:
        return self.ticker.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
