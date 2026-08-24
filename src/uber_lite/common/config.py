from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[3]

# Data Lake Storage Paths
BRONZE_PATH = str(BASE_DIR / "data_lake" / "bronze" / "taxi_trips")
SILVER_PATH = str(BASE_DIR / "data_lake" / "silver" / "taxi_trips")

# Streaming Checkpoint Paths
BRONZE_CHECKPOINT = str(BASE_DIR / "checkpoints" / "bronze" / "taxi_trips")
SILVER_CHECKPOINT = str(BASE_DIR / "checkpoints" / "silver" / "taxi_trips")
