"""Common SparkSession factory for DTCC Lakehouse jobs."""
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "DTCC-Lakehouse",
    driver_memory: str = "1500m",
) -> SparkSession:
    """Build or retrieve a memory-constrained local SparkSession.

    Respects container memory limits and configures minimal shuffle partitions
    for fast local development.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
