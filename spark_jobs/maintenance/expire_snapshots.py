#!/usr/bin/env python3
"""Phase 6: Iceberg Table Maintenance Lab - Expire Snapshots.

Demonstrates pruning historical Iceberg snapshots to prevent metadata bloat
while enforcing an audit retention window using Iceberg's native 'expire_snapshots' procedure.
Measures and prints before-and-after snapshot metadata counts.
"""
import argparse
import sys
from pyspark.sql import functions as F
from spark_jobs.common.spark_session import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Iceberg Expire Snapshots Lab")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    parser.add_argument(
        "--table",
        default="dtcc.gold_active_trades",
        help="Namespace and table name to expire snapshots for (e.g. dtcc.gold_active_trades)",
    )
    parser.add_argument(
        "--retain-last",
        type=int,
        default=2,
        help="Number of latest snapshots to retain (default: 2)",
    )
    return parser.parse_args()


def inspect_snapshots(spark, full_table_name: str):
    """Query the Iceberg '$snapshots' metadata table to report current commit history."""
    snapshots_df = spark.read.table(f"{full_table_name}.snapshots")
    count = snapshots_df.count()
    snapshots = snapshots_df.select(
        "snapshot_id", "parent_id", "operation", "committed_at"
    ).orderBy(F.col("committed_at").desc()).collect()
    
    return count, [s.asDict() for s in snapshots]


def main():
    args = parse_args()
    full_table = f"{args.catalog}.{args.table}"

    print(f"\n--- Iceberg Table Maintenance Lab: Expire Snapshots ({full_table}) ---")
    spark = get_spark_session(app_name="IcebergExpireSnapshotsLab")

    # 1. Inspect Snapshots Before Expiration
    print(f"\n1. Querying active snapshots BEFORE expiration on '{full_table}'...")
    count_before, snapshots_before = inspect_snapshots(spark, full_table)
    print(f"   • Total Active Snapshots: {count_before}")
    for s in snapshots_before:
        parent = s['parent_id'] if s['parent_id'] is not None else "ROOT"
        print(f"     - Snapshot {s['snapshot_id']} (Op: {s['operation']}, Parent: {parent}, Time: {s['committed_at']})")

    # 2. Execute expire_snapshots procedure
    print(f"\n2. Executing 'CALL {args.catalog}.system.expire_snapshots' (older_than => current_timestamp(), retain_last => {args.retain_last})...")
    expire_result = spark.sql(f"""
        CALL {args.catalog}.system.expire_snapshots(
            table => '{full_table}',
            older_than => current_timestamp(),
            retain_last => {args.retain_last}
        )
    """).collect()

    print("   Expiration Execution Summary:")
    for row in expire_result:
        row_dict = row.asDict()
        for k, v in row_dict.items():
            print(f"   • {k}: {v}")

    # 3. Inspect Snapshots After Expiration
    print(f"\n3. Querying active snapshots AFTER expiration on '{full_table}'...")
    count_after, snapshots_after = inspect_snapshots(spark, full_table)
    print(f"   • Total Active Snapshots: {count_after}")
    for s in snapshots_after:
        parent = s['parent_id'] if s['parent_id'] is not None else "ROOT"
        print(f"     - Snapshot {s['snapshot_id']} (Op: {s['operation']}, Parent: {parent}, Time: {s['committed_at']})")

    # 4. Verification Comparison
    print("\n--- Snapshot Retention Benchmark Results ---")
    print(f"   • Snapshots Before : {count_before}")
    print(f"   • Snapshots After  : {count_after}")
    print(f"   • Pruned Snapshots : {count_before - count_after} historical commits purged from catalog")

    spark.stop()
    print(f"\n--- Expire Snapshots Lab Completed Cleanly ({full_table}) ---\n")


if __name__ == "__main__":
    main()
