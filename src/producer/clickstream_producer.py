"""
Clickstream Event Producer.

Simulates realistic e-commerce clickstream events and publishes
them to a Kafka topic. Events include page views, product views,
add-to-cart actions, and purchases.
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.producer import config_loader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Event Templates
# ---------------------------------------------------------------------------

PRODUCTS = [
    {"id": "P001", "name": "Wireless Earbuds Pro", "category": "Electronics", "price": 79.99},
    {"id": "P002", "name": "Organic Cotton T-Shirt", "category": "Clothing", "price": 29.99},
    {"id": "P003", "name": "Smart LED Desk Lamp", "category": "Home & Garden", "price": 49.99},
    {"id": "P004", "name": "Python Data Engineering", "category": "Books", "price": 44.99},
    {"id": "P005", "name": "Yoga Mat Premium", "category": "Sports", "price": 34.99},
    {"id": "P006", "name": "Vitamin C Serum", "category": "Beauty", "price": 24.99},
    {"id": "P007", "name": "Mechanical Keyboard", "category": "Electronics", "price": 129.99},
    {"id": "P008", "name": "Running Shoes V2", "category": "Sports", "price": 89.99},
    {"id": "P009", "name": "Ceramic Plant Pot Set", "category": "Home & Garden", "price": 39.99},
    {"id": "P010", "name": "Data Pipelines Pocket Ref", "category": "Books", "price": 19.99},
]

PAGES = ["/", "/products", "/deals", "/about", "/blog", "/faq"]

EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "purchase",
]

# Probability weights for event type selection (funnel shape).
EVENT_WEIGHTS = [35, 30, 15, 5, 8, 7]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8)",
]

# ---------------------------------------------------------------------------
# Core Producer
# ---------------------------------------------------------------------------


class ClickstreamProducer:
    """Generates and publishes simulated clickstream events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "clickstream_events",
        num_users: int = 50,
    ) -> None:
        self.topic = topic
        self.num_users = num_users
        self._user_pool = [self._create_user() for _ in range(num_users)]

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
        logger.info(
            "Producer initialised — topic=%s, users=%d", self.topic, num_users
        )

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _create_user() -> dict[str, Any]:
        """Create a synthetic user profile."""
        user_id = f"U{uuid.uuid4().hex[:8].upper()}"
        return {
            "user_id": user_id,
            "session_id": str(uuid.uuid4()),
            "user_agent": random.choice(USER_AGENTS),
            "geo": random.choice(["KE", "US", "GB", "DE", "NG", "ZA", "IN"]),
        }

    def _generate_event(self) -> dict[str, Any]:
        """Build a single clickstream event."""
        user = random.choice(self._user_pool)
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        product = random.choice(PRODUCTS)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user["user_id"],
            "session_id": user["session_id"],
            "user_agent": user["user_agent"],
            "geo_country": user["geo"],
        }

        if event_type == "page_view":
            event["page"] = random.choice(PAGES)
        elif event_type in ("product_view", "add_to_cart", "remove_from_cart"):
            event["product_id"] = product["id"]
            event["product_name"] = product["name"]
            event["product_category"] = product["category"]
            event["product_price"] = product["price"]
        elif event_type in ("begin_checkout", "purchase"):
            cart_size = random.randint(1, 4)
            cart_items = random.sample(PRODUCTS, k=min(cart_size, len(PRODUCTS)))
            event["cart_items"] = [
                {"product_id": p["id"], "quantity": random.randint(1, 3), "price": p["price"]}
                for p in cart_items
            ]
            event["cart_total"] = round(
                sum(i["price"] * i["quantity"] for i in event["cart_items"]), 2
            )

        return event

    # -- public API -------------------------------------------------------

    def produce_event(self) -> dict[str, Any]:
        """Generate and send one event, returning the event dict."""
        event = self._generate_event()
        try:
            future = self.producer.send(
                self.topic,
                key=event["user_id"],
                value=event,
            )
            future.add_callback(lambda rm: logger.debug("Sent %s", event["event_id"]))
            future.add_errback(lambda exc: logger.error("Send failed: %s", exc))
        except KafkaError as exc:
            logger.error("Kafka error: %s", exc)
            raise
        return event

    def run(self, events_per_second: float = 5.0, max_events: int | None = None) -> None:
        """Continuously produce events at the given rate."""
        delay = 1.0 / events_per_second
        count = 0
        logger.info("Starting producer loop — %.1f events/sec", events_per_second)

        try:
            while max_events is None or count < max_events:
                event = self.produce_event()
                count += 1
                if count % 100 == 0:
                    logger.info("Produced %d events (latest: %s)", count, event["event_type"])
                time.sleep(delay + random.uniform(-delay * 0.2, delay * 0.2))
        except KeyboardInterrupt:
            logger.info("Producer stopped after %d events.", count)
        finally:
            self.producer.flush()
            self.producer.close()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the producer from command line."""
    import os
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    producer = ClickstreamProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "clickstream_events"),
    )
    producer.run(events_per_second=5.0)


if __name__ == "__main__":
    main()
