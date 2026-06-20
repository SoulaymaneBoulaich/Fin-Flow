"""
stock_producer.py — Fetches live stock prices from yFinance and publishes
tick events to the Kafka 'stock-ticks' topic.

Architecture:
 - One thread per ticker to avoid serial API latency.
 - Each fetch returns a TickEvent validated by Pydantic.
 - Published with ticker as the Kafka message key (ensures ordering per ticker).
 - Graceful shutdown on SIGINT / SIGTERM.
"""
from __future__ import annotations

import os
import sys
import signal
import time
import threading
import random
from datetime import datetime, timezone
from typing import Optional

import yfinance as yf
from loguru import logger

# ─── Local imports ────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from generation.schema.events import TickEvent
from ingestion.kafka.producer import FinFlowProducer

# ─── Configuration ────────────────────────────────────────────────────────────
TICKERS: list[str] = os.getenv("TICKERS", "AAPL,TSLA,AMZN,MSFT,GOOGL").split(",")
INTERVAL: int = int(os.getenv("TICK_INTERVAL_SECONDS", "5"))
TOPIC: str = os.getenv("KAFKA_TOPIC_STOCK_TICKS", "stock-ticks")
BROKERS: str = ",".join([
    os.getenv("KAFKA_BROKER_1", "localhost:29092"),
    os.getenv("KAFKA_BROKER_2", "localhost:29093"),
    os.getenv("KAFKA_BROKER_3", "localhost:29094"),
])

_shutdown = threading.Event()


_fallback_prices: dict[str, float] = {
    "AAPL": 185.0, "TSLA": 245.0, "AMZN": 191.0,
    "MSFT": 420.0, "GOOGL": 175.0, "NVDA": 900.0,
    "META": 530.0, "NFLX": 680.0
}


def _fetch_tick(ticker: str) -> Optional[TickEvent]:
    """Fetch the latest price for a single ticker. Falls back to simulation on failure."""
    try:
        tk = yf.Ticker(ticker)
        # fast_info is much faster than history() for live data
        info = tk.fast_info

        price = float(info.last_price)
        if price <= 0:
            raise ValueError(f"Received non-positive price: {price}")

        # yFinance fast_info doesn't always have open/high/low; fall back to price
        high = float(getattr(info, "year_high", price) or price)
        low = float(getattr(info, "year_low", price) or price)
        prev_close = float(getattr(info, "previous_close", price) or price)
        volume = int(getattr(info, "three_month_average_volume", 0) or 0)

        # Update fallback cache
        _fallback_prices[ticker] = price

        event = TickEvent(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            open=prev_close,
            high=max(prev_close, price, high),
            low=min(prev_close, price, low),
            close=price,
            volume=volume,
            vwap=price,
            source="yfinance",
        )
        return event

    except Exception as exc:
        logger.warning(f"[{ticker}] Failed to fetch live tick via yFinance ({exc}). Using simulated fallback.")
        base = _fallback_prices.get(ticker, 100.0)
        # Apply a small random walk change (-0.5% to +0.5%)
        change = random.uniform(-0.005, 0.005)
        new_price = round(base * (1 + change), 2)
        _fallback_prices[ticker] = new_price

        event = TickEvent(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc),
            open=round(base, 2),
            high=round(max(base, new_price) * 1.002, 2),
            low=round(min(base, new_price) * 0.998, 2),
            close=new_price,
            volume=random.randint(100_000, 1_000_000),
            vwap=new_price,
            source="simulated",
        )
        return event


def _producer_loop(ticker: str, producer: FinFlowProducer) -> None:
    """Continuously fetch and publish ticks for one ticker until shutdown."""
    logger.info(f"[{ticker}] Producer thread started. Interval={INTERVAL}s")
    while not _shutdown.is_set():
        start = time.monotonic()
        event = _fetch_tick(ticker)
        if event:
            producer.publish(
                topic=TOPIC,
                key=event.to_kafka_key(),
                value=event.to_kafka_value(),
            )
            logger.info(
                f"[{ticker}] Published tick → close={event.close:.2f}  "
                f"volume={event.volume:,}"
            )
        elapsed = time.monotonic() - start
        wait_time = max(0.0, INTERVAL - elapsed)
        _shutdown.wait(wait_time)

    logger.info(f"[{ticker}] Producer thread stopped.")


def _handle_signal(sig, frame):
    logger.info(f"Received signal {sig}. Initiating graceful shutdown...")
    _shutdown.set()


def main() -> None:
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(f"Starting FinFlow Stock Producer for tickers: {TICKERS}")
    logger.info(f"Kafka brokers: {BROKERS} | Topic: {TOPIC} | Interval: {INTERVAL}s")

    producer = FinFlowProducer(bootstrap_servers=BROKERS)

    threads = []
    for ticker in TICKERS:
        t = threading.Thread(
            target=_producer_loop,
            args=(ticker.strip(), producer),
            name=f"producer-{ticker}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Wait for shutdown signal
    _shutdown.wait()

    logger.info("Shutting down: flushing Kafka producer...")
    producer.flush()
    producer.close()

    for t in threads:
        t.join(timeout=5)

    logger.info("Stock producer stopped cleanly.")


if __name__ == "__main__":
    main()
