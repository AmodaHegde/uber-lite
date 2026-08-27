from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectBronze").getOrCreate()

path = "/home/amoda/uber-lite/data_lake/bronze/taxi_trips/ingestion_year=2026/ingestion_month=8/ingestion_day=16/part-00000-98fcd521-9945-46b4-8109-1b4cc1ee8edb.c000.snappy.parquet"

df = spark.read.parquet(path)

print(df.select("payment_type").distinct().show(truncate=False))
