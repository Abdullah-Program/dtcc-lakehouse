#!/usr/bin/env python3
"""Smoke test for Apache Polaris REST Catalog and Garage S3 in PySpark.

Verifies:
1. Polaris REST Catalog authentication and namespace creation (polaris.dtcc).
2. Iceberg table creation backed by Garage S3 (s3://dtcc-lakehouse/).
3. ACID insert and read-back integrity through Polaris.
4. Snapshot metadata inspection.
"""
from spark_jobs.common.spark_session import get_spark_session


def main():
    print("=" * 60)
    print("Initializing PySpark Session with Apache Polaris & Garage S3...")
    print("=" * 60)
    spark = get_spark_session(app_name="Polaris-Smoke-Test", enable_polaris=True)

    print("\n[Step 1] Creating namespace 'dtcc' in Polaris REST catalog...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.dtcc")
    namespaces = spark.sql("SHOW NAMESPACES IN polaris").collect()
    print(f"Namespaces in Polaris: {[row[0] for row in namespaces]}")

    print("\n[Step 2] Creating Iceberg table 'polaris.dtcc.smoke_rates' on S3...")
    spark.sql("DROP TABLE IF EXISTS polaris.dtcc.smoke_rates")
    spark.sql("""
        CREATE TABLE polaris.dtcc.smoke_rates (
            trade_key STRING,
            dissemination_id STRING,
            action_type STRING,
            notional DOUBLE,
            file_date STRING
        )
        USING iceberg
        PARTITIONED BY (file_date)
    """)
    print("Table polaris.dtcc.smoke_rates created successfully.")

    print("\n[Step 3] Inserting test derivative swap trade into Iceberg table...")
    spark.sql("""
        INSERT INTO polaris.dtcc.smoke_rates VALUES
            ('TK_10001', '10001', 'NEWT', 50000000.0, '2026-10-02'),
            ('TK_10002', '10002', 'NEWT', 75000000.0, '2026-10-02')
    """)

    print("\n[Step 4] Querying table through Polaris REST catalog...")
    df = spark.sql("SELECT * FROM polaris.dtcc.smoke_rates ORDER BY trade_key")
    df.show(truncate=False)
    count = df.count()
    print(f"Total rows retrieved from S3: {count}")
    assert count == 2, f"Expected 2 rows, found {count}"

    print("\n[Step 5] Inspecting Iceberg Snapshot metadata via Polaris...")
    snapshots_df = spark.sql("""
        SELECT snapshot_id, parent_id, operation, summary['total-records'] AS total_records
        FROM polaris.dtcc.smoke_rates.snapshots
    """)
    snapshots_df.show(truncate=False)

    print("=" * 60)
    print("ALL POLARIS + GARAGE S3 INTEGRATION CHECKS PASSED!")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
