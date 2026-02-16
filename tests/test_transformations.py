"""Tests for stream transformation functions."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

from src.transformations.aggregate_metrics import (
    compute_event_counts,
    compute_revenue_metrics,
    compute_funnel_metrics,
    sessionize,
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for testing."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestTransformations")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_events(spark):
    """Create a sample events DataFrame."""
    data = [
        ("e1", "page_view",    datetime(2025, 1, 1, 10, 0, 0), "U001", None),
        ("e2", "product_view", datetime(2025, 1, 1, 10, 0, 30), "U001", None),
        ("e3", "add_to_cart",  datetime(2025, 1, 1, 10, 1, 0), "U001", None),
        ("e4", "purchase",     datetime(2025, 1, 1, 10, 2, 0), "U001", 59.99),
        ("e5", "page_view",    datetime(2025, 1, 1, 10, 0, 10), "U002", None),
        ("e6", "product_view", datetime(2025, 1, 1, 10, 1, 0), "U002", None),
        ("e7", "page_view",    datetime(2025, 1, 1, 10, 5, 0), "U003", None),
    ]
    return spark.createDataFrame(
        data,
        ["event_id", "event_type", "event_timestamp", "user_id", "cart_total"],
    )


class TestComputeEventCounts:
    def test_counts_group_by_type(self, sample_events):
        result = compute_event_counts(sample_events, window="10 minutes")
        rows = result.collect()

        type_counts = {r["event_type"]: r["event_count"] for r in rows}
        assert type_counts["page_view"] == 3
        assert type_counts["product_view"] == 2
        assert type_counts["add_to_cart"] == 1
        assert type_counts["purchase"] == 1

    def test_output_columns(self, sample_events):
        result = compute_event_counts(sample_events, window="10 minutes")
        expected_cols = {"window_start", "window_end", "event_type", "event_count"}
        assert set(result.columns) == expected_cols


class TestComputeRevenueMetrics:
    def test_revenue_calculation(self, sample_events):
        result = compute_revenue_metrics(sample_events, window="10 minutes")
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["total_revenue"] == 59.99
        assert rows[0]["transaction_count"] == 1

    def test_no_purchases_returns_empty(self, spark):
        data = [
            ("e1", "page_view", datetime(2025, 1, 1, 10, 0, 0), "U001", None),
        ]
        df = spark.createDataFrame(
            data,
            ["event_id", "event_type", "event_timestamp", "user_id", "cart_total"],
        )
        result = compute_revenue_metrics(df, window="10 minutes")
        assert result.count() == 0


class TestComputeFunnelMetrics:
    def test_funnel_counts(self, sample_events):
        result = compute_funnel_metrics(sample_events, window="10 minutes")
        rows = result.collect()

        assert len(rows) == 1
        row = rows[0]
        assert row["page_views"] == 3
        assert row["product_views"] == 2
        assert row["add_to_carts"] == 1
        assert row["purchases"] == 1

    def test_conversion_rates(self, sample_events):
        result = compute_funnel_metrics(sample_events, window="10 minutes")
        row = result.collect()[0]

        assert row["view_to_cart_rate"] == 0.5      # 1/2
        assert row["cart_to_purchase_rate"] == 1.0   # 1/1


class TestSessionize:
    def test_assigns_sessions(self, spark):
        data = [
            ("e1", "page_view", datetime(2025, 1, 1, 10, 0, 0),  "U001", None),
            ("e2", "page_view", datetime(2025, 1, 1, 10, 5, 0),  "U001", None),
            ("e3", "page_view", datetime(2025, 1, 1, 11, 0, 0),  "U001", None),  # 55 min gap
        ]
        df = spark.createDataFrame(
            data,
            ["event_id", "event_type", "event_timestamp", "user_id", "cart_total"],
        )

        result = sessionize(df, timeout_minutes=30)
        rows = result.orderBy("event_timestamp").collect()

        assert rows[0]["session_number"] == 1
        assert rows[1]["session_number"] == 1
        assert rows[2]["session_number"] == 2  # new session after 55 min gap
