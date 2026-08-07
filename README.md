# Uber-Lite Telemetry Pipeline

## Overview
This project processes real-time vehicle location data. A Python script generates telemetry events. Apache Kafka transports the stream. Apache Spark Structured Streaming processes the messages and writes them into a Parquet data lake.

---

## Data Flow
1. **`producer.py`**: Generates simulated GPS coordinates, speed, and driver IDs, then sends JSON payloads to Kafka.
2. **Apache Kafka**: Receives events on topic `driver-telemetry` and orders messages by driver ID using keys.
3. **`consumer.py`**: Reads Kafka micro-batches using PySpark, transforms the data, and appends date fields.
4. **`data_lake/`**: Stores compressed Parquet files partitioned by year, month, and day.

---

## Requirements
* Operating System: Linux or WSL2 (Ubuntu)
* Python: 3.12+
* Docker Desktop (with Docker Compose)
* Poetry: 2.0+

---

## Setup & Running

### 1. Install Dependencies
```bash
poetry install

### 2. Start Kafka
```bash
docker compose -f kafka-stack-docker-compose/docker-compose.yml up -d

### 3. Run the pipeline:
In 2 terminals, run:
```bash
poetry run python producer.py
```bash
poetry run python consumer.py

### 4. Date Lake layout:
data_lake/
└── raw_telemetry/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── part-00000-....snappy.parquet
###5. Run code quality checks
```bash
poetry run black --check .
poetry run ruff check .


