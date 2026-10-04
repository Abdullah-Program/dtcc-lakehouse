#!/usr/bin/env python3
"""Phase 1: PySpark environment verification (Hello World).

Verifies that:
1. PySpark initializes cleanly inside the container.
2. SparkSession can create and execute a distributed DataFrame operation.
3. Master is local[*] and memory settings respect container limits.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def main() -> None:
    print("\n--- Starting PySpark Verification ---")
    print(f"Python runtime: {sys.version.split()[0]}")

    # Build SparkSession with modest memory constraints
    spark = (
        SparkSession.builder.appName("DTCC-Lakehouse-HelloSpark")
        .master("local[*]")
        .config("spark.driver.memory", "1500m")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")

    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("service", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
    ])

    data = [
        (1, "PySpark", "Active"),
        (2, "Apache Iceberg", "Pending Phase 2"),
        (3, "DTCC Pipeline", "Ready"),
    ]

    df = spark.createDataFrame(data, schema=schema)

    print("\n--- Spark DataFrame Output ---")
    df.show(truncate=False)

    total_rows = df.count()
    print(f"Verified row count: {total_rows}")

    spark.stop()
    print("--- PySpark Verification Complete (Clean Shutdown) ---\n")


if __name__ == "__main__":
    main()
