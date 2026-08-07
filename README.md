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
