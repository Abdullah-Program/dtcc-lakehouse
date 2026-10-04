"""Common SparkSession factory for DTCC Lakehouse jobs."""
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"


def get_spark_session(
    app_name: str = "DTCC-Lakehouse",
    driver_memory: str = "1500m",
    enable_iceberg: bool = True,
    enable_kafka: bool = False,
) -> SparkSession:
    """Build or retrieve a memory-constrained local SparkSession.

    Respects container memory limits and configures minimal shuffle partitions
    for fast local development. Configures Apache Iceberg extensions, local
    Hadoop catalog, and optional Spark-Kafka connector.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "2")
    )

    packages = []
    if enable_iceberg:
        packages.append(ICEBERG_PACKAGE)
    if enable_kafka:
        packages.append(KAFKA_PACKAGE)

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    if enable_iceberg:
        builder = (
            builder.config(
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
