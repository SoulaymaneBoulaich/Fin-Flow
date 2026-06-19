"""
test_events.py — Unit tests for Pydantic event schemas.
"""
import pytest
from datetime import datetime, timezone
from uuid import UUID

import sys
sys.path.insert(0, "src")

from generation.schema.events import TickEvent, TradeEvent, UserEvent

NOW = datetime.now(timezone.utc)


class TestTickEvent:
    def test_valid_tick(self):
        tick = TickEvent(
            ticker="AAPL",
            timestamp=NOW,
            open=180.0,
            high=185.0,
            low=179.5,
            close=184.0,
            volume=1_000_000,
            vwap=182.5,
        )
        assert tick.ticker == "AAPL"
        assert tick.close == 184.0
        assert tick.event_id  # auto-generated
        assert tick.source == "yfinance"

    def test_ticker_uppercased(self):
        tick = TickEvent(
            ticker="aapl", timestamp=NOW,
            open=1, high=2, low=0.5, close=1.5, volume=100
        )
        assert tick.ticker == "AAPL"

    def test_invalid_close_negative(self):
        with pytest.raises(Exception):
            TickEvent(
                ticker="AAPL", timestamp=NOW,
                open=1, high=2, low=0.5, close=-1, volume=100
            )

    def test_invalid_volume_negative(self):
        with pytest.raises(Exception):
            TickEvent(
                ticker="AAPL", timestamp=NOW,
                open=1, high=2, low=0.5, close=1.5, volume=-10
            )

    def test_close_zero_raises(self):
        """close must be > 0 (Pydantic gt=0 constraint)."""
        with pytest.raises(Exception):
            TickEvent(
                ticker="AAPL", timestamp=NOW,
                open=1, high=2, low=0.5, close=0.0, volume=100  # close == 0 invalid
            )

    def test_serialization(self):
        tick = TickEvent(
            ticker="AAPL", timestamp=NOW,
            open=1, high=2, low=0.5, close=1.5, volume=100
        )
        data = tick.model_dump()
        assert "event_id" in data
        assert "timestamp" in data
        assert data["ticker"] == "AAPL"

    def test_event_id_is_uuid(self):
        tick = TickEvent(
            ticker="AAPL", timestamp=NOW,
            open=1, high=2, low=0.5, close=1.5, volume=100
        )
        UUID(tick.event_id)  # Should not raise


class TestTradeEvent:
    def test_valid_trade(self):
        trade = TradeEvent(
            user_id="user_001",
            ticker="TSLA",
            action="BUY",
            quantity=10,
            price=250.0,
            total_value=2500.0,  # Must be provided and match quantity * price
        )
        assert trade.total_value == pytest.approx(2500.0)
        assert trade.action == "BUY"

    def test_invalid_action(self):
        with pytest.raises(Exception):
            TradeEvent(
                user_id="user_001", ticker="TSLA",
                action="HOLD", quantity=10, price=250.0, total_value=2500.0
            )

    def test_quantity_must_be_positive(self):
        with pytest.raises(Exception):
            TradeEvent(
                user_id="user_001", ticker="TSLA",
                action="BUY", quantity=0, price=250.0, total_value=0.0
            )

    def test_total_value_must_match_price_times_quantity(self):
        """total_value that doesn't match quantity*price should fail."""
        with pytest.raises(Exception):
            TradeEvent(
                user_id="user_001", ticker="TSLA",
                action="SELL", quantity=5, price=300.0,
                total_value=99.0,  # Wrong: should be 1500.0
            )

    def test_valid_sell(self):
        trade = TradeEvent(
            user_id="user_001", ticker="TSLA",
            action="SELL", quantity=5, price=300.0,
            total_value=1500.0,
        )
        assert trade.total_value == pytest.approx(1500.0)


class TestUserEvent:
    def test_valid_user_event(self):
        event = UserEvent(
            event_type="CREATED",
            full_name="Alice Smith",
            email="alice@example.com",
            date_of_birth="1990-05-15",
            country="US",
            account_type="PREMIUM",
        )
        assert event.event_type == "CREATED"
        assert event.account_type == "PREMIUM"

    def test_invalid_event_type(self):
        with pytest.raises(Exception):
            UserEvent(
                event_type="HACK",
                full_name="Bob", email="b@b.com",
                date_of_birth="1990-01-01", country="US",
                account_type="BASIC",
            )

    def test_invalid_account_type(self):
        with pytest.raises(Exception):
            UserEvent(
                event_type="CREATED",
                full_name="Bob", email="b@b.com",
                date_of_birth="1990-01-01", country="US",
                account_type="GOLD",  # Not a valid type
            )

    def test_auto_generated_user_id(self):
        event = UserEvent(
            event_type="CREATED",
            full_name="Bob Jones", email="bob@test.com",
            date_of_birth="1985-03-20", country="GB",
            account_type="BASIC",
        )
        assert event.user_id  # Auto-generated UUID
        UUID(event.user_id)   # Should be valid UUID
