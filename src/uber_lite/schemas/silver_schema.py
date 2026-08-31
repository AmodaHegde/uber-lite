from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Chicago coordinate bounding box limits
MIN_LATITUDE = 41.60
MAX_LATITUDE = 42.05
MIN_LONGITUDE = -87.95
MAX_LONGITUDE = -87.50

# Target Silver Delta Schema
SILVER_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("taxi_id", StringType(), True),
        StructField("trip_start_timestamp", TimestampType(), False),
        StructField("trip_end_timestamp", TimestampType(), True),
        StructField("trip_seconds", IntegerType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("pickup_census_tract", StringType(), True),
        StructField("dropoff_census_tract", StringType(), True),
        StructField("pickup_community_area", IntegerType(), True),
        StructField("dropoff_community_area", IntegerType(), True),
        StructField("fare", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("extras", DoubleType(), True),
        StructField("trip_total", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("company", StringType(), True),
        StructField("pickup_centroid_latitude", DoubleType(), True),
        StructField("pickup_centroid_longitude", DoubleType(), True),
        StructField("pickup_centroid_location", StringType(), True),
        StructField("dropoff_centroid_latitude", DoubleType(), True),
        StructField("dropoff_centroid_longitude", DoubleType(), True),
        StructField("dropoff_centroid_location", StringType(), True),
        StructField("speed_mph", DoubleType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("kafka_record_timestamp", TimestampType(), True),
        StructField("ingestion_date", DateType(), True),
        StructField("trip_year", IntegerType(), False),
        StructField("trip_month", IntegerType(), False),
    ]
)
