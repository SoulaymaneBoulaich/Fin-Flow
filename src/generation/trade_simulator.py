"""
trade_simulator.py — Simulates realistic trade events using statistical distributions.

Trade size distribution: Log-Normal (most trades small, occasional large blocks)
Trade arrival: Poisson process (random arrivals at a known average rate)
Published to Kafka 'trade-events' topic.
"""
from __future__ import annotations

import os
import sys
import math
import random
import signal
import threading
import time
from datetime import datetime, timezone

import numpy as np
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from generation.schema.events import TradeEvent
from generation.synthetic_users import get_random_user, get_all_users
from ingestion.kafka.producer import FinFlowProducer

# ─── Configuration ────────────────────────────────────────────────────────────
TICKERS: list[str] = os.getenv("TICKERS", "AAPL,TSLA,AMZN,MSFT,GOOGL").split(",")
TOPIC: str = os.getenv("KAFKA_TOPIC_TRADE_EVENTS", "trade-events")
TRADES_PER_MINUTE: int = int(os.getenv("TRADE_EVENTS_PER_MINUTE", "30"))
BROKERS: str = ",".join([
    os.getenv("KAFKA_BROKER_1", "localhost:29092"),
    os.getenv("KAFKA_BROKER_2", "localhost:29093"),
    os.getenv("KAFKA_BROKER_3", "localhost:29094"),
])

# Statistical parameters for trade size (log-normal distribution)
# Mean trade size ≈ e^(mu + sigma²/2)
# With mu=3.5, sigma=1.2 → mean ≈ 60 shares, most trades 5-200 shares
_LN_MU: float = 3.5
_LN_SIGMA: float = 1.2

# Price simulation: within ±3% of a base price per ticker
_BASE_PRICES: dict[str, float] = {
    "AAPL": 185.0, "TSLA": 245.0, "AMZN": 190.0,
    "MSFT": 420.0, "GOOGL": 175.0, "NVDA": 900.0,
    "META": 530.0, "NFLX": 680.0,
}

_shutdown = threading.Event()


def _simulate_price(ticker: str) -> float:
    """Simulate a realistic price with ±3% random walk from base."""
    base = _BASE_PRICES.get(ticker, 100.0)
    pct_change = np.random.normal(0, 0.01)  # 1% std deviation
    return round(base * (1 + pct_change), 2)


def _simulate_quantity() -> int:
    """Draw trade quantity from log-normal distribution."""
    qty = int(np.random.lognormal(mean=_LN_MU, sigma=_LN_SIGMA))
    return max(1, qty)


def _generate_trade(user_id: str) -> TradeEvent:
    """Generate a single trade event for the given user."""
    ticker = random.choice([t.strip() for t in TICKERS])
    action = random.choices(["BUY", "SELL"], weights=[0.55, 0.45])[0]
    quantity = _simulate_quantity()
    price = _simulate_price(ticker)
    total_value = round(quantity * price, 4)

    return TradeEvent(
        user_id=user_id,
        ticker=ticker,
        action=action,
        quantity=quantity,
        price=price,
        total_value=total_value,
        timestamp=datetime.now(timezone.utc),
    )


def _simulator_loop(producer: FinFlowProducer) -> None:
    """
    Main simulation loop.
    Uses Poisson process: inter-arrival times are Exponential(λ).
    λ = TRADES_PER_MINUTE / 60 trades per second.
    """
    lam = TRADES_PER_MINUTE / 60.0  # trades/second
    logger.info(
        f"Trade simulator started. Rate={TRADES_PER_MINUTE}/min "
        f"(λ={lam:.3f}/sec)"
    )

    while not _shutdown.is_set():
        # Exponential inter-arrival time for Poisson process
        wait_time = np.random.exponential(scale=1.0 / lam)

        if _shutdown.wait(wait_time):
            break  # shutdown event set

        try:
            user = get_random_user()
        except RuntimeError:
            logger.warning("Users not initialized yet. Waiting...")
            time.sleep(2)
            continue

        # Only trade if user has analytics_consent (simulate consent check)
        if not user.analytics_consent:
            continue

        trade = _generate_trade(user.user_id)
        producer.publish(
            topic=TOPIC,
            key=trade.to_kafka_key(),
            value=trade.to_kafka_value(),
        )
        logger.debug(
            f"Trade: {trade.action} {trade.quantity} {trade.ticker} "
            f"@ ${trade.price:.2f} (user={trade.user_id[:8]}...)"
        )

    logger.info("Trade simulator stopped.")


def _handle_signal(sig, frame):
    logger.info(f"Signal {sig} received. Stopping trade simulator...")
    _shutdown.set()


def main() -> None:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer = FinFlowProducer(bootstrap_servers=BROKERS)

    thread = threading.Thread(
        target=_simulator_loop,
        args=(producer,),
        name="trade-simulator",
        daemon=True,
    )
    thread.start()

    _shutdown.wait()
    producer.flush()
    producer.close()
    thread.join(timeout=5)
    logger.info("Trade simulator exited cleanly.")


if __name__ == "__main__":
    # Quick test: generate 5 trades
    np.random.seed(42)
    for _ in range(5):
        trade = _generate_trade(user_id="test-user-uuid")
        print(trade.model_dump_json(indent=2))
