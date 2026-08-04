from pyspark.sql import SparkSession

#Initialise a spark session
spark = (
    SparkSession.builder.appName("uber-lite-consumer")
    .config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)
