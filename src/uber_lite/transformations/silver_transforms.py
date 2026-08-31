from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    month,
    regexp_replace,
    round,
    to_timestamp,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.types import DoubleType, IntegerType

from uber_lite.schemas.silver_schema import (
    MAX_LATITUDE,
    MAX_LONGITUDE,
    MIN_LATITUDE,
    MIN_LONGITUDE,
)


def parse_and_cast_types(df: DataFrame) -> DataFrame:
    # 1. Strip currency characters and cast numeric columns
    monetary_cols = ["fare", "tips", "tolls", "extras", "trip_total"]
    for col_name in monetary_cols:
        df = df.withColumn(
            col_name,
            regexp_replace(col(col_name), r"[\$,]", "").cast(DoubleType()),
        )

    # 2. Parse timestamps and numeric types from exact Bronze column names
    return (
        df.withColumn(
            "trip_start_timestamp",
            to_timestamp(col("trip_start_timestamp"), "MM/dd/yyyy hh:mm:ss a"),
        )
        .withColumn(
            "trip_end_timestamp",
            to_timestamp(col("trip_end_timestamp"), "MM/dd/yyyy hh:mm:ss a"),
        )
        .withColumn(
            "ingestion_timestamp",
            to_timestamp(col("ingestion_timestamp")),
        )
        .withColumn("trip_seconds", col("trip_seconds").cast(IntegerType()))
        .withColumn("trip_miles", col("trip_miles").cast(DoubleType()))
        .withColumn(
            "pickup_community_area",
            col("pickup_community_area").cast(IntegerType()),
        )
        .withColumn(
            "dropoff_community_area",
            col("dropoff_community_area").cast(IntegerType()),
        )
        .withColumn(
            "pickup_centroid_latitude",
            col("pickup_centroid_latitude").cast(DoubleType()),
        )
        .withColumn(
            "pickup_centroid_longitude",
            col("pickup_centroid_longitude").cast(DoubleType()),
        )
        .withColumn(
            "dropoff_centroid_latitude",
            col("dropoff_centroid_latitude").cast(DoubleType()),
        )
        .withColumn(
            "dropoff_centroid_longitude",
            col("dropoff_centroid_longitude").cast(DoubleType()),
        )
        .withColumn("payment_type", upper(trim(col("payment_type"))))
        .withColumn("company", trim(col("company")))
    )


def enrich_derived_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "speed_mph",
            when(
                col("trip_seconds") > 0,
                round(col("trip_miles") / (col("trip_seconds") / 3600.0), 2),
            ).otherwise(0.0),
        )
        .withColumn("hour_of_day", hour(col("trip_start_timestamp")))
        .withColumn("day_of_week", dayofweek(col("trip_start_timestamp")))
        .withColumn(
            "is_weekend",
            when(col("day_of_week").isin(1, 7), True).otherwise(False),
        )
        .withColumn("trip_year", year(col("trip_start_timestamp")))
        .withColumn("trip_month", month(col("trip_start_timestamp")))
    )


def apply_data_quality_filters(df: DataFrame) -> DataFrame:
    # Coordinate boundary verification
    valid_pickup_coords = col("pickup_centroid_latitude").isNull() | (
        col("pickup_centroid_latitude").between(MIN_LATITUDE, MAX_LATITUDE)
        & col("pickup_centroid_longitude").between(MIN_LONGITUDE, MAX_LONGITUDE)
    )

    valid_dropoff_coords = col("dropoff_centroid_latitude").isNull() | (
        col("dropoff_centroid_latitude").between(MIN_LATITUDE, MAX_LATITUDE)
        & col("dropoff_centroid_longitude").between(MIN_LONGITUDE, MAX_LONGITUDE)
    )

    return (
        df.filter(col("trip_id").isNotNull())
        .filter(col("trip_start_timestamp").isNotNull())
        .filter(col("trip_seconds") >= 0)
        .filter(col("trip_miles") >= 0.0)
        .filter(col("fare") >= 0.0)
        .filter(col("trip_total") >= 0.0)
        .filter(col("speed_mph") <= 120.0)
        .filter(valid_pickup_coords)
        .filter(valid_dropoff_coords)
    )


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    parsed_df = parse_and_cast_types(df)
    enriched_df = enrich_derived_features(parsed_df)
    validated_df = apply_data_quality_filters(enriched_df)

    deduped_df = validated_df.withWatermark(
        "trip_start_timestamp", "2 hours"
    ).dropDuplicates(["trip_id", "trip_start_timestamp"])

    return deduped_df.select(
        "trip_id",
        "taxi_id",
        "trip_start_timestamp",
        "trip_end_timestamp",
        "trip_seconds",
        "trip_miles",
        "pickup_census_tract",
        "dropoff_census_tract",
        "pickup_community_area",
        "dropoff_community_area",
        "fare",
        "tips",
        "tolls",
        "extras",
        "trip_total",
        "payment_type",
        "company",
        "pickup_centroid_latitude",
        "pickup_centroid_longitude",
        "pickup_centroid_location",
        "dropoff_centroid_latitude",
        "dropoff_centroid_longitude",
        "dropoff_centroid_location",
        "speed_mph",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "ingestion_timestamp",
        "kafka_record_timestamp",
        "ingestion_date",
        "trip_year",
        "trip_month",
    )
