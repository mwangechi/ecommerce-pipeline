"""Tests for the clickstream event producer."""

import json
import pytest
from unittest.mock import MagicMock, patch

from src.producer.clickstream_producer import (
    ClickstreamProducer,
    EVENT_TYPES,
    PRODUCTS,
)


class TestClickstreamProducer:
    """Unit tests for ClickstreamProducer."""

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_init_creates_producer(self, mock_kafka_cls):
        """Producer initialises with correct Kafka settings."""
        producer = ClickstreamProducer(
            bootstrap_servers="test:9092",
            topic="test_topic",
            num_users=5,
        )
        assert producer.topic == "test_topic"
        assert len(producer._user_pool) == 5
        mock_kafka_cls.assert_called_once()

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_generate_event_structure(self, mock_kafka_cls):
        """Generated events have required fields."""
        producer = ClickstreamProducer(topic="test", num_users=3)
        event = producer._generate_event()

        assert "event_id" in event
        assert "event_type" in event
        assert "timestamp" in event
        assert "user_id" in event
        assert event["event_type"] in EVENT_TYPES

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_purchase_event_has_cart(self, mock_kafka_cls):
        """Purchase events include cart_items and cart_total."""
        producer = ClickstreamProducer(topic="test", num_users=3)

        # Generate events until we get a purchase
        for _ in range(500):
            event = producer._generate_event()
            if event["event_type"] == "purchase":
                assert "cart_items" in event
                assert "cart_total" in event
                assert event["cart_total"] > 0
                assert len(event["cart_items"]) >= 1
                return

        pytest.skip("No purchase event generated in 500 attempts")

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_page_view_has_page(self, mock_kafka_cls):
        """Page view events include a page path."""
        producer = ClickstreamProducer(topic="test", num_users=3)

        for _ in range(500):
            event = producer._generate_event()
            if event["event_type"] == "page_view":
                assert "page" in event
                assert event["page"].startswith("/")
                return

        pytest.skip("No page_view event generated in 500 attempts")

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_produce_event_sends_to_kafka(self, mock_kafka_cls):
        """produce_event sends the event to the Kafka topic."""
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_instance.send.return_value = mock_future
        mock_kafka_cls.return_value = mock_instance

        producer = ClickstreamProducer(topic="test_topic", num_users=3)
        event = producer.produce_event()

        mock_instance.send.assert_called_once()
        call_kwargs = mock_instance.send.call_args
        assert call_kwargs[1]["key"] == event["user_id"]

    @patch("src.producer.clickstream_producer.KafkaProducer")
    def test_create_user_format(self, mock_kafka_cls):
        """Created users have properly formatted IDs."""
        user = ClickstreamProducer._create_user()
        assert user["user_id"].startswith("U")
        assert len(user["user_id"]) == 9  # U + 8 hex chars
        assert "session_id" in user
        assert "geo" in user
