"""
synthetic_users.py — Generates realistic synthetic user profiles using Faker.

Each user is a UserEvent (Pydantic-validated) that contains PII fields used to
demonstrate masking, tokenization, and GDPR compliance in later layers.

Published to Kafka 'user-events' topic.
"""
from __future__ import annotations

import os
import sys
import random
import threading
from datetime import datetime, timezone

from faker import Faker
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from generation.schema.events import UserEvent
from ingestion.kafka.producer import FinFlowProducer

# ─── Configuration ────────────────────────────────────────────────────────────
USER_COUNT: int = int(os.getenv("SYNTHETIC_USERS_COUNT", "1000"))
TOPIC: str = os.getenv("KAFKA_TOPIC_USER_EVENTS", "user-events")
BROKERS: str = ",".join([
    os.getenv("KAFKA_BROKER_1", "localhost:29092"),
    os.getenv("KAFKA_BROKER_2", "localhost:29093"),
    os.getenv("KAFKA_BROKER_3", "localhost:29094"),
])

fake = Faker(["en_US", "en_GB", "de_DE", "fr_FR", "es_ES"])

_ACCOUNT_TYPES = ["BASIC", "PREMIUM", "PROFESSIONAL"]
_ACCOUNT_WEIGHTS = [0.6, 0.3, 0.1]  # Most users are BASIC

# Country → ISO 3166-1 alpha-2
_COUNTRIES = ["US", "GB", "DE", "FR", "ES", "NL", "CA", "AU", "JP", "SG"]

# Store generated users in memory so trade simulator can reference them
_generated_users: list[UserEvent] = []
_users_lock = threading.Lock()


def generate_user() -> UserEvent:
    """Generate a single realistic synthetic user."""
    dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
    account_type = random.choices(_ACCOUNT_TYPES, weights=_ACCOUNT_WEIGHTS, k=1)[0]

    return UserEvent(
        event_type="CREATED",
        full_name=fake.name(),
        email=fake.email(),
        date_of_birth=dob.isoformat(),
        country=random.choice(_COUNTRIES),
        account_type=account_type,
        marketing_consent=random.choice([True, False]),
        analytics_consent=random.choice([True, True, False]),  # 2/3 consent
        created_at=datetime.now(timezone.utc),
        timestamp=datetime.now(timezone.utc),
    )


def generate_users_batch(count: int) -> list[UserEvent]:
    """Generate a batch of users."""
    users = [generate_user() for _ in range(count)]
    return users


def publish_users(
    producer: FinFlowProducer,
    users: list[UserEvent],
    topic: str = TOPIC,
) -> None:
    """Publish a list of users to Kafka."""
    for user in users:
        producer.publish(
            topic=topic,
            key=user.to_kafka_key(),
            value=user.to_kafka_value(),
        )
    producer.flush()
    logger.info(f"Published {len(users)} user events to topic '{topic}'")


def initialize_users(producer: FinFlowProducer) -> list[UserEvent]:
    """
    Generate USER_COUNT synthetic users, publish them, and store them globally
    so the trade simulator can reference them.
    """
    global _generated_users
    logger.info(f"Generating {USER_COUNT} synthetic users...")
    users = generate_users_batch(USER_COUNT)

    with _users_lock:
        _generated_users = users

    publish_users(producer, users)
    logger.info(f"Initialized {USER_COUNT} synthetic users.")
    return users


def get_random_user() -> UserEvent:
    """Return a random user from the global pool (after initialization)."""
    with _users_lock:
        if not _generated_users:
            raise RuntimeError("Users not initialized. Call initialize_users() first.")
        return random.choice(_generated_users)


def get_all_users() -> list[UserEvent]:
    """Return all generated users."""
    with _users_lock:
        return list(_generated_users)


if __name__ == "__main__":
    # Standalone test: generate and print 5 users
    users = generate_users_batch(5)
    for u in users:
        print(u.model_dump_json(indent=2))
