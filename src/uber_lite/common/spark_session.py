from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "UberLite") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.13:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
