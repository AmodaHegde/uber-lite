from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Bronze-Trips-Query")
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

# Load the Delta table and register it as an SQL view
delta_df = spark.read.format("delta").load("data_lake/bronze/taxi_trips")
delta_df.createOrReplaceTempView("bronze_trips")

# Run standard SQL queries
result = spark.sql("""
    SELECT
    `trip_miles`
    FROM bronze_trips
    WHERE TRY_CAST(`trip_seconds` AS DOUBLE) < 100
""")

result.show()
