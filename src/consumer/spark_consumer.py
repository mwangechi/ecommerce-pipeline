"""
Spark Structured Streaming Consumer.

Reads clickstream events from Kafka, applies windowed aggregations
and sessionization, then writes metrics to PostgreSQL.
"""

import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    ArrayType,
    IntegerType,
)

from src.transformations.aggregate_metrics import (
    add_watermark,
    compute_event_counts,
    compute_revenue_metrics,
    compute_funnel_metrics,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

CART_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("page", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("cart_items", ArrayType(CART_ITEM_SCHEMA), True),
    StructField("cart_total", DoubleType(), True),
])


# ---------------------------------------------------------------------------
# Core Consumer
# ---------------------------------------------------------------------------


class SparkStreamConsumer:
    """Consumes clickstream events from Kafka using Spark Structured Streaming."""

    def __init__(
        self,
        spark: SparkSession | None = None,
        kafka_servers: str = "localhost:9092",
        kafka_topic: str = "clickstream_events",
        checkpoint_dir: str = "./checkpoints",
    ) -> None:
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.checkpoint_dir = checkpoint_dir

        self.spark = spark or self._create_spark_session()
        logger.info("SparkStreamConsumer initialised — topic=%s", kafka_topic)

    @staticmethod
    def _create_spark_session() -> SparkSession:
        """Build a local Spark session with Kafka support."""
        return (
            SparkSession.builder
            .appName("EcommerceStreamProcessor")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .getOrCreate()
        )

    # -- reading ----------------------------------------------------------

    def read_stream(self) -> DataFrame:
        """Read raw JSON events from Kafka as a streaming DataFrame."""
        raw = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_servers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("event_timestamp", F.to_timestamp("timestamp"))
            .drop("timestamp")
        )

        return parsed

    # -- writing ----------------------------------------------------------

    def write_to_postgres(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """Write a batch DataFrame to PostgreSQL."""
        jdbc_url = (
            f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}"
            f":{os.getenv('POSTGRES_PORT', '5432')}"
            f"/{os.getenv('POSTGRES_DB', 'ecommerce')}"
        )
        properties = {
            "user": os.getenv("POSTGRES_USER", "pipeline"),
            "password": os.getenv("POSTGRES_PASSWORD", "changeme_in_production"),
            "driver": "org.postgresql.Driver",
        }
        df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)

    @staticmethod
    def _foreach_batch_writer(table_name: str):
        """Return a foreachBatch function that writes to Postgres."""
        def _write(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.count() == 0:
                return
            jdbc_url = (
                f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}"
                f":{os.getenv('POSTGRES_PORT', '5432')}"
                f"/{os.getenv('POSTGRES_DB', 'ecommerce')}"
            )
            props = {
                "user": os.getenv("POSTGRES_USER", "pipeline"),
                "password": os.getenv("POSTGRES_PASSWORD", "changeme_in_production"),
                "driver": "org.postgresql.Driver",
            }
            batch_df.write.jdbc(
                url=jdbc_url, table=table_name, mode="append", properties=props
            )
            logger.info("Batch %d: wrote %d rows → %s", batch_id, batch_df.count(), table_name)
        return _write

    # -- pipeline ---------------------------------------------------------

    def run(self) -> None:
        """Execute the full streaming pipeline."""
        events = self.read_stream()
        watermarked = add_watermark(events, delay="5 minutes")

        # 1. Event counts per window
        event_counts = compute_event_counts(watermarked, window="1 minute")
        q1 = (
            event_counts.writeStream
            .outputMode("update")
            .foreachBatch(self._foreach_batch_writer("event_counts"))
            .option("checkpointLocation", f"{self.checkpoint_dir}/event_counts")
            .trigger(processingTime="10 seconds")
            .start()
        )

        # 2. Revenue metrics per window
        revenue = compute_revenue_metrics(watermarked, window="5 minutes")
        q2 = (
            revenue.writeStream
            .outputMode("update")
            .foreachBatch(self._foreach_batch_writer("revenue_metrics"))
            .option("checkpointLocation", f"{self.checkpoint_dir}/revenue")
            .trigger(processingTime="10 seconds")
            .start()
        )

        # 3. Funnel metrics per window
        funnel = compute_funnel_metrics(watermarked, window="10 minutes")
        q3 = (
            funnel.writeStream
            .outputMode("update")
            .foreachBatch(self._foreach_batch_writer("funnel_metrics"))
            .option("checkpointLocation", f"{self.checkpoint_dir}/funnel")
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info("All streaming queries started. Awaiting termination…")
        self.spark.streams.awaitAnyTermination()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    consumer = SparkStreamConsumer(
        kafka_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=os.getenv("KAFKA_TOPIC", "clickstream_events"),
    )
    consumer.run()


if __name__ == "__main__":
    main()
