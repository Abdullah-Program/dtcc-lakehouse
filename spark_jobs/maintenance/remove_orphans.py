#!/usr/bin/env python3
"""Phase 6: Iceberg Table Maintenance Lab - Remove Orphan Files.

Demonstrates identifying and purging unreferenced 'zombie' data files
from Garage S3 using Iceberg's native 'remove_orphan_files' procedure.
Runs a dry-run check first, followed by the purge operation.
"""
import argparse
import sys
from spark_jobs.common.spark_session import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Iceberg Remove Orphan Files Lab")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    parser.add_argument(
        "--table",
        default="dtcc.gold_active_trades",
        help="Namespace and table name to inspect for orphan files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, only identify orphan files without deleting them",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    full_table = f"{args.catalog}.{args.table}"

    print(f"\n--- Iceberg Table Maintenance Lab: Remove Orphan Files ({full_table}) ---")
    spark = get_spark_session(app_name="IcebergRemoveOrphansLab")

    # 1. First run as dry_run to safely audit what files would be removed
    print(f"\n1. Auditing orphan files (dry_run => true)...")
    dry_run_sql = f"""
        CALL {args.catalog}.system.remove_orphan_files(
            table => '{full_table}',
            dry_run => true
        )
    """
    dry_run_results = spark.sql(dry_run_sql).collect()
    print(f"   • Orphan candidate files found: {len(dry_run_results)}")
    for row in dry_run_results[:10]:
        print(f"     - {row['orphan_file_location']}")
    if len(dry_run_results) > 10:
        print(f"     ... and {len(dry_run_results) - 10} more files.")

    # 2. Execute actual purge if not dry_run
    if not args.dry_run and dry_run_results:
        print(f"\n2. Executing actual purge (dry_run => false)...")
        purge_sql = f"""
            CALL {args.catalog}.system.remove_orphan_files(
                table => '{full_table}',
                dry_run => false
            )
        """
        purge_results = spark.sql(purge_sql).collect()
        print(f"   • Successfully purged {len(purge_results)} orphan files from storage!")
    elif not dry_run_results:
        print("   • Table storage is clean! No unreferenced orphan files found.")
    else:
        print("\n   [Dry Run Mode] No files were deleted.")

    spark.stop()
    print(f"\n--- Remove Orphan Files Lab Completed Cleanly ({full_table}) ---\n")


if __name__ == "__main__":
    main()
