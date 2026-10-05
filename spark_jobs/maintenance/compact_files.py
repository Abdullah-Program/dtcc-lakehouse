#!/usr/bin/env python3
"""Phase 6: Iceberg Table Maintenance Lab - File Compaction.

Demonstrates solving the "Small Files Problem" caused by streaming ingestion
and micro-batches by using Apache Iceberg's native 'rewrite_data_files' procedure.
Measures and prints before-and-after data file counts and average file sizes.
"""
import argparse
import sys
from pyspark.sql import functions as F
from spark_jobs.common.spark_session import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Iceberg Table Compaction Lab")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    parser.add_argument(
        "--table",
        default="dtcc.silver_rates",
        help="Namespace and table name to compact (e.g. dtcc.silver_rates)",
    )
    return parser.parse_args()


def inspect_files_metadata(spark, full_table_name: str):
    """Query the Iceberg '$files' metadata table to report current data file metrics."""
    files_df = spark.read.table(f"{full_table_name}.files")
    
    metrics = files_df.select(
        F.count("file_path").alias("file_count"),
        F.sum("file_size_in_bytes").alias("total_bytes"),
        F.avg("file_size_in_bytes").alias("avg_bytes"),
        F.min("file_size_in_bytes").alias("min_bytes"),
        F.max("file_size_in_bytes").alias("max_bytes"),
    ).collect()[0]

    return {
        "file_count": metrics["file_count"] or 0,
        "total_bytes": metrics["total_bytes"] or 0,
        "avg_bytes": round(metrics["avg_bytes"] or 0, 1),
        "min_bytes": metrics["min_bytes"] or 0,
        "max_bytes": metrics["max_bytes"] or 0,
    }


def main():
    args = parse_args()
    full_table = f"{args.catalog}.{args.table}"

    print(f"\n--- Iceberg Table Maintenance Lab: Compaction ({full_table}) ---")
    spark = get_spark_session(app_name="IcebergCompactionLab")

    # 1. Inspect Files Before Compaction
    print(f"\n1. Measuring data files BEFORE compaction on '{full_table}'...")
    before = inspect_files_metadata(spark, full_table)
    print(f"   • Total Data Files : {before['file_count']}")
    print(f"   • Total Size (KB)  : {before['total_bytes'] / 1024:.2f} KB")
    print(f"   • Avg File Size    : {before['avg_bytes'] / 1024:.2f} KB")
    print(f"   • Min / Max Size   : {before['min_bytes'] / 1024:.2f} KB / {before['max_bytes'] / 1024:.2f} KB")

    # 2. Execute rewrite_data_files procedure
    print(f"\n2. Executing 'CALL {args.catalog}.system.rewrite_data_files' (Strategy: binpack)...")
    compaction_result = spark.sql(f"""
        CALL {args.catalog}.system.rewrite_data_files(
            table => '{full_table}',
            strategy => 'binpack',
            options => map('min-input-files', '2')
        )
    """).collect()

    print("   Compaction Execution Summary:")
    for row in compaction_result:
        row_dict = row.asDict()
        for k, v in row_dict.items():
            print(f"   • {k}: {v}")

    # 3. Inspect Files After Compaction
    print(f"\n3. Measuring data files AFTER compaction on '{full_table}'...")
    after = inspect_files_metadata(spark, full_table)
    print(f"   • Total Data Files : {after['file_count']}")
    print(f"   • Total Size (KB)  : {after['total_bytes'] / 1024:.2f} KB")
    print(f"   • Avg File Size    : {after['avg_bytes'] / 1024:.2f} KB")
    print(f"   • Min / Max Size   : {after['min_bytes'] / 1024:.2f} KB / {after['max_bytes'] / 1024:.2f} KB")

    # 4. Verification Comparison
    file_diff = before['file_count'] - after['file_count']
    print("\n--- Compaction Benchmark Results ---")
    print(f"   • Files Before : {before['file_count']}")
    print(f"   • Files After  : {after['file_count']}")
    print(f"   • Net Reduction: {file_diff} small files merged")

    spark.stop()
    print(f"\n--- Compaction Lab Completed Cleanly ({full_table}) ---\n")


if __name__ == "__main__":
    main()
