import json
import random
import time
import uuid
from datetime import UTC, datetime

from kafka import KafkaProducer
from sodapy import Socrata

client = Socrata(
    "data.cityofnewyork.us",
    "VrIlUBJWAJoFck1mgPFU2v2nQ",
    username="itsahegde@gmail.com",
    password="ucc6WMEOX7FRPK",
)

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
        return random.choices(["ON_TRIP", "AVAILABLE"], weights=[0.8, 0.2])[0]
    elif current_status == "AVAILABLE":
        return random.choices(
            ["ON_TRIP", "AVAILABLE", "OFFLINE"], weights=[0.5, 0.4, 0.1]
        )[0]
    else:
        return random.choices(["AVAILABLE", "OFFLINE"], weights=[0.7, 0.3])[0]


def generate_telemetry_ping(driver_id):
    state = driver_fleet[driver_id]
    new_status = update_driver_status(state["status"])
    state["status"] = new_status

    if new_status != "OFFLINE":
        state["lat"] += random.uniform(-0.001, 0.001)
        state["lon"] += random.uniform(-0.001, 0.001)

    speed = round(random.uniform(15, 55), 1) if new_status != "OFFLINE" else 0.0

    payload = {
        "event_id": str(uuid.uuid4()),
        "driver_id": driver_id,
        "timestamp": int(datetime.now(UTC).timestamp()),
        "location": {
            "latitude": round(state["lat"], 6),
            "longitude": round(state["lon"], 6),
        },
        "status": new_status,
        "speed_kmh": speed,
    }
    return payload


if __name__ == "__main__":
    print(f"Starting producer for topic: '{TOPIC_NAME}'")

    try:
        while True:
            for driver_id in driver_fleet:
                telemetry = generate_telemetry_ping(driver_id)
                producer.send(topic=TOPIC_NAME, key=driver_id, value=telemetry)
                print(
                    f"[{telemetry['status']:^9}] Driver: {driver_id} | "
                    f"Lat/Lon: ({telemetry['location']['latitude']}, {telemetry['location']['longitude']}) | "
                    f"Speed: {telemetry['speed_kmh']} km/h"
                )

            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down Producer cleanly...")
        producer.flush()
        producer.close()
        print("Done.")
