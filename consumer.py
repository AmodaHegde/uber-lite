import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

# 1. Match package version to local PySpark
pyspark_version = pyspark.__version__
scala_version = "2.13" if pyspark_version.startswith("4") else "2.12"
kafka_package = (
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{pyspark_version}"
)

# 2. Initialize SparkSession
spark = (
    SparkSession.builder.appName("UberTelemetryConsumer")
    .config("spark.jars.packages", kafka_package)
    # Configure local directory access
    .config("spark.sql.warehouse.dir", "file:///" + os.path.abspath("spark-warehouse").replace("\\", "/"))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- WINDOWS HADOOP NativeIO PATCH ---
if os.name == "nt":
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.permissions.umask-mode", "000"
    )

# 3. Define Schema
telemetry_schema = (
    StructType()
    .add("event_id", StringType())
    .add("driver_id", StringType())
    .add("timestamp", IntegerType())
    .add(
        "location",
        StructType()
        .add("latitude", DoubleType())
        .add("longitude", DoubleType()),
    )
    .add("status", StringType())
    .add("speed_kmh", DoubleType())
)

# 4. Read Streaming Source from Kafka
raw_kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "driver-telemetry")
    .option("startingOffsets", "latest")
    .load()
)

# 5. Deserialize JSON & Format Date/Time Columns
parsed_telemetry_df = (
    raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
    .select(F.from_json(F.col("json_payload"), telemetry_schema).alias("data"))
    .select("data.*")
    .withColumn(
        "event_time", F.from_unixtime(F.col("timestamp")).cast("timestamp")
    )
    .withColumn("year", F.year(F.col("event_time")))
    .withColumn("month", F.month(F.col("event_time")))
    .withColumn("day", F.dayofmonth(F.col("event_time")))
)


# 6. Multi-Sink Micro-Batch Callback
def process_micro_batch(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return

    print(f"Micro-Batch ID: {batch_id} | Rows: {micro_batch_df.count()}")

    # Path 1: Append Parquet files to Data Lake
    data_lake_path = "./data_lake/raw_telemetry/"
    (
        micro_batch_df.write.mode("append")
        .partitionBy("year", "month", "day")
        .parquet(data_lake_path)
    )
    print(f"✅ [Data Lake] Appended batch to {data_lake_path}")


# 7. Start Query Stream
if __name__ == "__main__":
    query = (
        parsed_telemetry_df.writeStream.foreachBatch(process_micro_batch)
        .option("checkpointLocation", "./checkpoints/telemetry_consumer/")
        .start()
    )

    query.awaitTermination()