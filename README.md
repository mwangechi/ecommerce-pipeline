[Back to Portfolio](https://mwangechi.github.io/mwangechi.github.io/)

# Real-Time E-Commerce Analytics Pipeline

A production-grade streaming data pipeline that ingests clickstream events via Apache Kafka, processes them with Apache Spark Structured Streaming, and loads aggregated metrics into PostgreSQL for real-time dashboarding.

## Architecture

```
┌──────────────┐     ┌──────────┐     ┌──────────────────┐     ┌────────────┐
│  Clickstream │────▶│  Kafka   │────▶│  Spark Streaming │────▶│ PostgreSQL │
│  Producer    │     │  Broker  │     │  (Transforms)    │     │  (Metrics) │
└──────────────┘     └──────────┘     └──────────────────┘     └────────────┘
                                              │
                                     ┌────────┴────────┐
                                     │  Airflow DAG    │
                                     │  (Orchestration)│
                                     └─────────────────┘
```

## Features

- **Event Simulation**: Realistic clickstream event producer (page views, add-to-cart, purchases)
- **Stream Processing**: Spark Structured Streaming with windowed aggregations
- **Sessionization**: Groups events into user sessions with configurable timeout
- **Funnel Analytics**: Tracks conversion funnel (view → cart → purchase)
- **Orchestration**: Airflow DAG for scheduling and monitoring
- **Data Quality**: Schema validation and late-arrival handling

## Tech Stack

| Component | Technology |
|---|---|
| Message Broker | Apache Kafka 3.x |
| Stream Processing | Apache Spark 3.5 (Structured Streaming) |
| Orchestration | Apache Airflow 2.x |
| Data Store | PostgreSQL 15 |
| Language | Python 3.11 |
| Containerization | Docker & Docker Compose |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+

### 1. Clone & Configure

```bash
git clone https://github.com/wamahua/ecommerce-pipeline.git
cd ecommerce-pipeline
cp .env.example .env
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

This starts Kafka, Zookeeper, Spark, and PostgreSQL.

### 3. Initialize Database

```bash
docker compose exec postgres psql -U pipeline -d ecommerce -f /schemas/create_tables.sql
```

### 4. Run the Pipeline

```bash
# Install dependencies
pip install -r requirements.txt

# Start the clickstream producer
python -m src.producer.clickstream_producer

# In another terminal, start Spark consumer
python -m src.consumer.spark_consumer
```

### 5. Run Tests

```bash
pytest tests/ -v
```

## Project Structure

```
ecommerce-pipeline/
├── README.md
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── config/
│   └── pipeline_config.yaml
├── dags/
│   └── ecommerce_dag.py
├── src/
│   ├── __init__.py
│   ├── producer/
│   │   ├── __init__.py
│   │   └── clickstream_producer.py
│   ├── consumer/
│   │   ├── __init__.py
│   │   └── spark_consumer.py
│   └── transformations/
│       ├── __init__.py
│       └── aggregate_metrics.py
├── schemas/
│   └── create_tables.sql
└── tests/
    ├── __init__.py
    ├── test_producer.py
    └── test_transformations.py
```

## Configuration

All pipeline settings are managed via `config/pipeline_config.yaml` and environment variables in `.env`.

| Variable | Description | Default |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC` | Clickstream events topic | `clickstream_events` |
| `SPARK_MASTER` | Spark master URL | `local[*]` |
| `POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `POSTGRES_DB` | Target database | `ecommerce` |

## License

MIT
