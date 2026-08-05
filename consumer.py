from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

#Initialise a spark session
spark = (
    SparkSession.builder.appName("uber-lite-consumer")
    .config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Schema definition for incoming kafka stream

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
    .add("speed_kmh", IntegerType())
)

#read stream from kafka
raw_kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "{}") # fill in with kafka topic name
    .option("startingOffsets", "latest")
    .load()
)

#deserialise json and parse for date/time
parsed_telemetry_df = (
    raw_kafka_df.selectExpr("CAST(value as STRING) as json_payload")
    .select(F.from_json(F.col("json_payload"), telemetry_schema).alias("data"))
    .select("data.*")

    .withColumn(
        "event_time", F.from_unixtime(F.col("timestamp")).cast("timestamp")
    )

    .withColumn("year", F.year(F.col("event_time")))
    .withColumn("month", F.month(F.col("event_time")))
    .withColumn("day", F.day(F.col("event_time")))
)