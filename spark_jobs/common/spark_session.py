"""Common SparkSession factory for DTCC Lakehouse jobs."""
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def get_spark_session(
    app_name: str = "DTCC-Lakehouse",
    driver_memory: str = "1500m",
    enable_iceberg: bool = True,
) -> SparkSession:
    """Build or retrieve a memory-constrained local SparkSession.

    Respects container memory limits and configures minimal shuffle partitions
    for fast local development. Configures Apache Iceberg extensions and local
    Hadoop catalog when enable_iceberg is True.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "2")
    )

    if enable_iceberg:
        builder = (
            builder.config("spark.jars.packages", ICEBERG_PACKAGE)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.local.type", "hadoop")
            .config(
                "spark.sql.catalog.local.warehouse",
                "/workspace/warehouse/iceberg",
            )
            .config("spark.sql.defaultCatalog", "local")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
