"""
Streaming Aggregation & Transformation Logic.

Pure Spark DataFrame transformations for windowed event counts,
revenue metrics, and conversion funnel analysis.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_watermark(df: DataFrame, delay: str = "5 minutes") -> DataFrame:
    """Apply a watermark on ``event_timestamp`` for late-arrival handling."""
    return df.withWatermark("event_timestamp", delay)


def compute_event_counts(df: DataFrame, window: str = "1 minute") -> DataFrame:
    """
    Count events by type in tumbling windows.

    Returns columns: window_start, window_end, event_type, event_count
    """
    return (
        df.groupBy(
            F.window("event_timestamp", window).alias("event_window"),
            "event_type",
        )
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("event_window.start").alias("window_start"),
            F.col("event_window.end").alias("window_end"),
            "event_type",
            "event_count",
        )
    )


def compute_revenue_metrics(df: DataFrame, window: str = "5 minutes") -> DataFrame:
    """
    Aggregate revenue from purchase events per window.

    Returns columns: window_start, window_end, total_revenue,
                     transaction_count, avg_order_value
    """
    purchases = df.filter(F.col("event_type") == "purchase")

    return (
        purchases.groupBy(
            F.window("event_timestamp", window).alias("rev_window"),
        )
        .agg(
            F.sum("cart_total").alias("total_revenue"),
            F.count("*").alias("transaction_count"),
            F.avg("cart_total").alias("avg_order_value"),
        )
        .select(
            F.col("rev_window.start").alias("window_start"),
            F.col("rev_window.end").alias("window_end"),
            F.round("total_revenue", 2).alias("total_revenue"),
            "transaction_count",
            F.round("avg_order_value", 2).alias("avg_order_value"),
        )
    )


def compute_funnel_metrics(df: DataFrame, window: str = "10 minutes") -> DataFrame:
    """
    Compute conversion funnel metrics per window.

    Tracks:  page_view → product_view → add_to_cart → purchase

    Returns columns: window_start, window_end, page_views2, product_views,
                     add_to_carts, purchases, view_to_cart_rate, cart_to_purchase_rate
    """
    return (
        df.groupBy(
            F.window("event_timestamp", window).alias("funnel_window"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        )
        .withColumn(
            "view_to_cart_rate",
            F.round(
                F.when(F.col("product_views") > 0, F.col("add_to_carts") / F.col("product_views"))
                .otherwise(0.0),
                4,
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.round(
                F.when(F.col("add_to_carts") > 0, F.col("purchases") / F.col("add_to_carts"))
                .otherwise(0.0),
                4,
            ),
        )
        .select(
            F.col("funnel_window.start").alias("window_start"),
            F.col("funnel_window.end").alias("window_end"),
            "page_views",
            "product_views",
            "add_to_carts",
            "purchases",
            "view_to_cart_rate",
            "cart_to_purchase_rate",
        )
    )


def sessionize(df: DataFrame, timeout_minutes: int = 30) -> DataFrame:
    """
    Assign session boundaries based on an inactivity gap.

    A new session starts when the gap between consecutive events
    for the same ``user_id`` exceeds *timeout_minutes*.

    Returns the DataFrame with an added ``session_number`` column.
    """
    user_window = Window.partitionBy("user_id").orderBy("event_timestamp")

    return (
        df.withColumn("prev_ts", F.lag("event_timestamp").over(user_window))
        .withColumn(
            "gap_minutes",
            (F.unix_timestamp("event_timestamp") - F.unix_timestamp("prev_ts")) / 60,
        )
        .withColumn(
            "new_session",
            F.when(
                (F.col("prev_ts").isNull()) | (F.col("gap_minutes") > timeout_minutes),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "session_number",
            F.sum("new_session").over(user_window),
        )
        .drop("prev_ts", "gap_minutes", "new_session")
    )
