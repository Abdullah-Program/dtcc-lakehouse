"""Common SparkSession factory for DTCC Lakehouse jobs."""
import os
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
ICEBERG_AWS_PACKAGE = "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"

# Default local Garage S3 / Polaris settings
POLARIS_URI = os.getenv("POLARIS_URI", "http://polaris:8181/api/catalog")
POLARIS_WAREHOUSE = os.getenv("POLARIS_WAREHOUSE", "dtcc_catalog")
POLARIS_CREDENTIAL = os.getenv("POLARIS_CREDENTIAL", "root:s3cr3t")
GARAGE_S3_ENDPOINT = os.getenv("GARAGE_S3_ENDPOINT", "http://garage:3900")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "GK135da1acc67448e5f988c2d9")
AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    "8ebd487cc54261411c33d336465f8a26ba58c064bddb3c56692189b2b4a21eba",
)


def get_spark_session(
    app_name: str = "DTCC-Lakehouse",
    driver_memory: str = "1500m",
    enable_iceberg: bool = True,
    enable_kafka: bool = False,
    enable_polaris: bool = True,
) -> SparkSession:
    """Build or retrieve a memory-constrained local SparkSession.

    Respects container memory limits and configures minimal shuffle partitions
    for fast local development. Configures Apache Iceberg extensions, local
    Hadoop catalog, optional Apache Polaris REST catalog with Garage S3,
    and optional Spark-Kafka connector.
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
        if enable_polaris:
            packages.append(ICEBERG_AWS_PACKAGE)
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
            # Local Hadoop catalog (for Phases 1-4 backward compatibility)
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

        # Apache Polaris REST catalog (Phase 5 decoupled architecture)
        if enable_polaris:
            builder = (
                builder.config(
                    "spark.sql.catalog.polaris",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config("spark.sql.catalog.polaris.type", "rest")
                .config("spark.sql.catalog.polaris.uri", POLARIS_URI)
                .config(
                    "spark.sql.catalog.polaris.oauth2-server-uri",
                    f"{POLARIS_URI}/v1/oauth/tokens",
                )
                .config("spark.sql.catalog.polaris.rest.auth.type", "oauth2")
                .config("spark.sql.catalog.polaris.warehouse", POLARIS_WAREHOUSE)
                .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
                .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
                .config("spark.sql.catalog.polaris.token-refresh-enabled", "true")
                .config("spark.sql.catalog.polaris.header.Polaris-Realm", "POLARIS")
                .config(
                    "spark.sql.catalog.polaris.metrics-reporter-impl",
                    "org.apache.iceberg.metrics.LoggingMetricsReporter",
                )
                .config(
                    "spark.sql.catalog.polaris.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
                .config(
                    "spark.sql.catalog.polaris.s3.endpoint",
                    GARAGE_S3_ENDPOINT,
                )
                .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
                .config(
                    "spark.sql.catalog.polaris.s3.access-key-id",
                    AWS_ACCESS_KEY_ID,
                )
                .config(
                    "spark.sql.catalog.polaris.s3.secret-access-key",
                    AWS_SECRET_ACCESS_KEY,
                )
                .config("spark.sql.catalog.polaris.client.region", "garage")
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
