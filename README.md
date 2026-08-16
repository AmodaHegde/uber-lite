# Uber-Lite Data Streaming Pipeline

## Overview

This project streams transportation event data using a **Medallion Lakehouse Architecture**.

A Python producer reads historical **Chicago Taxi Trip** records and streams them to **Apache Kafka**. Apache Spark Structured Streaming consumes the stream, parses incoming payloads against a strict schema, and writes raw transactional records into a **Bronze Delta Lake** table.

## Data Flow

1. **`producer.py`** — Reads raw trip records line-by-line from local storage, attaches an `ingestion_timestamp`, and publishes JSON payloads to the Kafka topic `raw_chicago_trips`.
2. **Apache Kafka** — Serves as the distributed message broker, decoupling data generation from downstream stream consumption.
3. **`consumer.py`** — Consumes Kafka micro-batches using PySpark Structured Streaming, parses JSON records, extracts date partition attributes, and appends data to Delta storage.
4. **`data_lake/bronze/`** — Stores raw ACID-compliant Delta Lake tables with metadata tracking through `_delta_log/`.

## Requirements

* **Operating System:** Linux or WSL2 (Ubuntu)
* **Java:** OpenJDK 8, 11, or 17
* **Python:** 3.12+
* **Docker:** Docker Desktop with Docker Compose
* **Poetry:** 2.0+

## Setup & Running

### 1. Install Dependencies

```bash
poetry install
```

### 2. Start Kafka

```bash
docker compose -f kafka-stack-docker-compose/docker-compose.yml up -d
```

### 3. Run the Pipeline

Run the producer and consumer in two separate terminals.

**Terminal 1 — Start Producer**

```bash
poetry run python producer.py
```

**Terminal 2 — Start Bronze Consumer**

```bash
poetry run python consumer.py
```

## Data Lake Layout

```text
uber-lite/
├── checkpoints/
│   └── bronze/
│       └── taxi_trips/
│           ├── commits/
│           ├── offsets/
│           ├── sources/
│           └── metadata
│
└── data_lake/
    └── bronze/
        └── taxi_trips/
            ├── _delta_log/
            │   └── 00000000000000000000.json
            │
            └── ingestion_year=YYYY/
                └── ingestion_month=M/
                    └── ingestion_day=D/
                        └── part-00000-....snappy.parquet
```

## Code Quality Checks

Run the following commands to check formatting and linting:

```bash
poetry run black .
poetry run ruff check --fix
```
