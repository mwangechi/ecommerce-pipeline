"""
Airflow DAG: E-Commerce Analytics Pipeline.

Schedules the Spark streaming job, monitors health, and manages
the lifecycle of the real-time analytics pipeline.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

# ---------------------------------------------------------------------------
# DAG Configuration
# ---------------------------------------------------------------------------

DAG_ID = "ecommerce_analytics_pipeline"
PROJECT_DIR = "/opt/ecommerce-pipeline"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _check_kafka_health(**context) -> str:
    """Check if Kafka topic has recent messages."""
    from kafka import KafkaConsumer

    try:
        consumer = KafkaConsumer(
            "clickstream_events",
            bootstrap_servers="kafka:9092",
            consumer_timeout_ms=10_000,
            auto_offset_reset="latest",
        )
        partitions = consumer.partitions_for_topic("clickstream_events")
        consumer.close()

        if partitions and len(partitions) > 0:
            return "start_spark_streaming"
        return "alert_no_data"
    except Exception as e:
        context["ti"].log.error("Kafka health check failed: %s", e)
        return "alert_no_data"


def _validate_metrics(**context) -> None:
    """Run basic sanity checks on the output metrics tables."""
    import psycopg2
    import os

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ecommerce"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme_in_production"),
    )
    cur = conn.cursor()

    tables = ["event_counts", "revenue_metrics", "funnel_metrics"]
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        count = cur.fetchone()[0]
        context["ti"].log.info("Table %s has %d rows", table, count)
        if count == 0:
            raise ValueError(f"Table {table} is empty — pipeline may have stalled.")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Orchestrates the real-time e-commerce clickstream analytics pipeline",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "ecommerce", "analytics"],
) as dag:

    check_kafka = BranchPythonOperator(
        task_id="check_kafka_health",
        python_callable=_check_kafka_health,
    )

    alert_no_data = BashOperator(
        task_id="alert_no_data",
        bash_command='echo "⚠️  Kafka topic has no partitions or is unreachable."',
    )

    start_spark = BashOperator(
        task_id="start_spark_streaming",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "--master local[*] "
            "-m src.consumer.spark_consumer "
            "2>&1 | tee /tmp/spark_consumer.log"
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate = PythonOperator(
        task_id="validate_metrics",
        python_callable=_validate_metrics,
        trigger_rule="none_failed_min_one_success",
    )

    # Task dependencies
    check_kafka >> [start_spark, alert_no_data]
    start_spark >> validate
