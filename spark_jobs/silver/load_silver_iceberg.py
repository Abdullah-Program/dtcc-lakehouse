#!/usr/bin/env python3
"""Phase 2, Step 2.2: Load cleaned Silver data into Apache Iceberg table.

Reads cleaned Silver Parquet from /workspace/warehouse/silver/rates,
and writes to the Iceberg table 'local.dtcc.silver_rates' partitioned by file_date.
Verifies row counts and inspects Iceberg metadata (snapshots, files, partitions).
"""
import argparse
import sys
from pathlib import Path
from pyspark.sql import functions as F
from spark_jobs.common.spark_session import get_spark_session

SILVER_PARQUET_DIR = Path("/workspace/warehouse/silver/rates")


def parse_args():
    parser = argparse.ArgumentParser(description="Load cleaned Silver data into Iceberg table.")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    table_name = f"{catalog}.dtcc.silver_rates"
    namespace = f"{catalog}.dtcc"

    print(f"\n--- Loading Cleaned Silver Data into Apache Iceberg ({table_name}) ---")
    spark = get_spark_session(
        app_name=f"DTCC-Silver-LoadIceberg-{catalog}",
        enable_iceberg=True,
        enable_polaris=(catalog == "polaris"),
    )

    # 1. Ensure the Iceberg namespace exists
    print(f"1. Ensuring Iceberg namespace '{namespace}' exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    # 2. Read the cleaned Silver Parquet records
    print(f"2. Reading Silver Parquet from {SILVER_PARQUET_DIR}...")
    df_silver = spark.read.parquet(str(SILVER_PARQUET_DIR))
    source_count = df_silver.count()
    print(f"Loaded {source_count:,} cleaned records from Parquet.")

    # 3. Write to Iceberg table partitioned by file_date
    print(f"3. Writing to Iceberg table '{table_name}' partitioned by file_date...")
    (
        df_silver.writeTo(table_name)
        .tableProperty("write.format.default", "parquet")
        .partitionedBy("file_date")
        .createOrReplace()
    )
    print("Write to Iceberg table complete.")

    # 4. Verify row count from the Iceberg table
    print(f"\n4. Verifying record count in '{table_name}'...")
    df_iceberg = spark.table(table_name)
    iceberg_count = df_iceberg.count()
    print(f"Iceberg table count: {iceberg_count:,}")

    assert iceberg_count == source_count, (
        f"Row count mismatch! Source Parquet: {source_count}, Iceberg: {iceberg_count}"
    )
    print("Verified: Row count matches source Parquet exactly.")

    # 5. Inspect Iceberg Snapshot Metadata
    print(f"\n5. Querying Iceberg Snapshots Metadata ({table_name}.snapshots):")
    df_snapshots = spark.sql(f"""
        SELECT 
            snapshot_id, 
            parent_id, 
            operation, 
            summary['total-records'] AS total_records,
            summary['added-data-files'] AS added_data_files,
            committed_at
        FROM {table_name}.snapshots
    """)
    df_snapshots.show(truncate=False)

    # 6. Inspect Iceberg Data Files Metadata
    print(f"6. Querying Iceberg Data Files Metadata ({table_name}.files):")
    df_files = spark.sql(f"""
        SELECT 
            file_path, 
            file_format, 
            partition.file_date AS partition_date, 
            record_count, 
            file_size_in_bytes
        FROM {table_name}.files
    """)
    df_files.show(truncate=False)

    # 7. Preview sample records from Iceberg table
    print("7. Previewing sample records from Iceberg table:")
    df_iceberg.select(
        "file_date",
        "trade_key",
        "dissemination_identifier",
        "action_type",
        "event_timestamp",
        "notional_amount_leg_1",
        "is_capped_notional_leg_1",
    ).show(5, truncate=False)

    spark.stop()
    print(f"--- Iceberg Silver Loading PASSED Successfully ({table_name}) ---\n")


if __name__ == "__main__":
    main()
