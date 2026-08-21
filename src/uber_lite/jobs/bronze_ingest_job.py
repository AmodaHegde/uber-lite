from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, from_json, month, to_date, year
from pyspark.sql.types import StringType, StructField, StructType

# 1. Initialize Spark Session with Delta Lake & Kafka dependencies
spark = (
    SparkSession.builder.appName("UberLite-Bronze-Consumer")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.3.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# Set logging level to WARN to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# 2. Define strict raw schema (all string fields to preserve raw data as-is)
raw_schema = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("taxi_id", StringType(), True),
        StructField("trip_start_timestamp", StringType(), True),
        StructField("trip_end_timestamp", StringType(), True),
        StructField("trip_seconds", StringType(), True),
        StructField("trip_miles", StringType(), True),
        StructField("pickup_census_tract", StringType(), True),
        StructField("dropoff_census_tract", StringType(), True),
        StructField("pickup_community_area", StringType(), True),
        StructField("dropoff_community_area", StringType(), True),
        StructField("fare", StringType(), True),
        StructField("tips", StringType(), True),
        StructField("tolls", StringType(), True),
        StructField("extras", StringType(), True),
        StructField("trip_total", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("company", StringType(), True),
        StructField("pickup_centroid_latitude", StringType(), True),
        StructField("pickup_centroid_longitude", StringType(), True),
        StructField("pickup_centroid_location", StringType(), True),
        StructField("dropoff_centroid_latitude", StringType(), True),
        StructField("dropoff_centroid_longitude", StringType(), True),
        StructField("dropoff_centroid_location", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)

# 3. Read stream from Kafka topic
kafka_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "driver_telemetry")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Parse JSON payload and extract metadata + partition date columns
parsed_df = (
    kafka_stream_df.selectExpr(
        "CAST(value AS STRING) as json_payload", "timestamp as kafka_record_timestamp"
    )
    .select(
        from_json(col("json_payload"), raw_schema).alias("data"),
        col("kafka_record_timestamp"),
    )
    .select("data.*", "kafka_record_timestamp")
    .withColumn("ingestion_date", to_date(col("ingestion_timestamp")))
    .withColumn("ingestion_year", year(col("ingestion_date")))
    .withColumn("ingestion_month", month(col("ingestion_date")))
    .withColumn("ingestion_day", dayofmonth(col("ingestion_date")))
)

# 5. Define paths
bronze_table_path = "data_lake/bronze/taxi_trips"
checkpoint_path = "checkpoints/bronze/taxi_trips"

# 6. Write stream to Delta Lake partitioned by ingestion date parts
query = (
    parsed_df.writeStream.format("delta")
    .outputMode("append")
    .partitionBy("ingestion_year", "ingestion_month", "ingestion_day")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 seconds")
    .start(bronze_table_path)
)

print(f"[INFO] Bronze streaming query active. Writing to: {bronze_table_path}")
query.awaitTermination()
