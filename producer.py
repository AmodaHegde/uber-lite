import csv
import json
import signal
from datetime import UTC, datetime

from confluent_kafka import KafkaError
from kafka import KafkaProducer

# Global flag to handle graceful shutdown
running = True


def handle_shutdown(sig, frame):
    print("\n[INFO] Shutdown signal received. Stopping producer...")


signal.signal(signal.SIGINT, handle_shutdown)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def stream_raw_taxi_data(file_path, topic_name):
    producer = create_producer()
    total_records = 0

    print(f"[INFO] Streaming from {file_path} to topic '{topic_name}'...")

    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                if not running:
                    break

                row["ingestion_timestamp"] = datetime.now(UTC).isoformat()

                producer.send(topic_name, value=row)
                total_records += 1

                # Print progress every 1,000 records
                if total_records % 1000 == 0:
                    print(f"[INFO] Sent {total_records} records to Kafka...")

    except KafkaError as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

    finally:
        print("[INFO] Flushing queued messages and closing producer...")
        producer.flush()
        producer.close()
        print(f"[INFO] Producer stopped. Total records streamed: {total_records}")


if __name__ == "__main__":
    DATA_FILE = "/mnt/d/Downloads/taxi_part__1.csv"
    TOPIC = "driver_telemetry"
    stream_raw_taxi_data(DATA_FILE, TOPIC)
