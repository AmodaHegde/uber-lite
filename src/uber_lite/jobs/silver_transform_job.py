from uber_lite.common.config import (
    BRONZE_PATH,
    SILVER_CHECKPOINT,
    SILVER_PATH,
)
from uber_lite.common.spark_session import get_spark_session
from uber_lite.transformations.silver_transforms import transform_bronze_to_silver


def run_silver_pipeline():
    spark = get_spark_session("UberLite-Silver-Pipeline")
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Initializing Bronze Delta Stream reader from: {BRONZE_PATH}")
    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    print("[INFO] Applying Silver transformations and data quality rules...")
    silver_stream = transform_bronze_to_silver(bronze_stream)

    print(f"[INFO] Streaming to Silver Delta Lake: {SILVER_PATH}")
    query = (
        silver_stream.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("trip_year", "trip_month")
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_silver_pipeline()
