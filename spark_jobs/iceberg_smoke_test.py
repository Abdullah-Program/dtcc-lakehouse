#!/usr/bin/env python3
"""Smoke test to verify Apache Iceberg integration with PySpark.

Validates:
1. Iceberg Spark runtime jar resolution.
2. IcebergSparkSessionExtensions SQL parsing.
3. Local HadoopCatalog creation in warehouse/iceberg/.
4. Snapshot generation and table metadata inspection.
"""
from spark_jobs.common.spark_session import get_spark_session


def main() -> None:
    print("\n--- Initializing Spark with Apache Iceberg Support ---")
    spark = get_spark_session(app_name="DTCC-Iceberg-SmokeTest", enable_iceberg=True)

    print("1. Creating Iceberg namespace 'dtcc'...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.dtcc")

    print("2. Creating Iceberg table 'local.dtcc.smoke_test'...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.dtcc.smoke_test (
            id STRING,
            symbol STRING,
            price DOUBLE,
            created_at TIMESTAMP
        ) USING iceberg
    """)

    print("3. Inserting test trade record...")
    spark.sql("""
        INSERT INTO local.dtcc.smoke_test
        VALUES ('T1', 'SOFR_SWAP', 5.25, current_timestamp())
    """)

    print("4. Querying Iceberg table content:")
    df_rows = spark.sql("SELECT * FROM local.dtcc.smoke_test")
    df_rows.show(truncate=False)

    print("5. Querying Iceberg Metadata (Snapshots):")
    df_snapshots = spark.sql("""
        SELECT snapshot_id, parent_id, operation, summary['total-records'] as total_records
        FROM local.dtcc.smoke_test.snapshots
    """)
    df_snapshots.show(truncate=False)

    print("6. Cleaning up test table...")
    spark.sql("DROP TABLE local.dtcc.smoke_test")

    spark.stop()
    print("--- Apache Iceberg Smoke Test PASSED Successfully! ---\n")


if __name__ == "__main__":
    main()
