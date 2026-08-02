import datetime
import json
import random
import time
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

TOPIC_NAME = "driver-telemetry"

driver_fleet = {
    "drv_101": {"lat": 12.9716, "lon": 77.5946, "status": "AVAILABLE"},
    "drv_102": {"lat": 12.9352, "lon": 77.6245, "status": "ON_TRIP"},
    "drv_103": {"lat": 12.9141, "lon": 77.6387, "status": "AVAILABLE"},
    "drv_104": {"lat": 12.9611, "lon": 77.6412, "status": "ON_TRIP"},
    "drv_105": {"lat": 12.9223, "lon": 77.5811, "status": "OFFLINE"},
}

def update_driver_status(current_status):
    if current_status == "ON_TRIP":
        return random.choices(
            ["ON_TRIP", "AVAILABLE"], weights=[0.8, 0.2]
        )[0]
    elif current_status == "AVAILABLE":
            return random.choices(
                ["ON_TRIP", "AVAILABLE", "OFFLINE"], weights=[0.5, 0.4, 0.1]
            )[0]
    else:
        return random.choices(
            ["AVAILABLE", "OFFLINE"], weights=[0.7, 0.3]
        )[0]

def generate_telemetry_ping(driver_id):
     return None