#!/usr/bin/env python3
"""Phase 3: Trade Corrections Engine (Gold Layer).

Reads cleaned events from Iceberg Silver table 'local.dtcc.silver_rates',
deduplicates the latest state per trade_key within the batch,
and applies stateful reconciliation into 'local.dtcc.gold_active_trades'
using Apache Iceberg's native SQL MERGE INTO.
"""
import argparse
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_jobs.common.spark_session import get_spark_session


def parse_args():
    parser = argparse.ArgumentParser(description="Trade Corrections Engine (Gold Layer)")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    return parser.parse_args()


def map_lifecycle_status_expr() -> F.Column:
    """Return a Column expression mapping action_type to lifecycle_status.

    Rules:
    - EROR (Error/Cancel): Trade was reported in error -> CANCELLED
    - TERM (Termination): Trade was unwound or settled -> TERMINATED
    - NEWT, MODI, CORR, REVI: Active trade contract   -> ACTIVE
    """
    return (
        F.when(F.col("action_type") == "EROR", F.lit("CANCELLED"))
        .when(F.col("action_type") == "TERM", F.lit("TERMINATED"))
        .when(F.col("action_type").isin("NEWT", "MODI", "CORR", "REVI"), F.lit("ACTIVE"))
        .otherwise(F.lit("UNKNOWN"))
    )


def deduplicate_latest_batch(df: DataFrame) -> DataFrame:
    """Select the latest record per trade_key based on dissemination_identifier.

    Dissemination Identifier is strictly monotonically increasing with message sequence.
    Deduplicating to the latest message per key prevents MERGE INTO duplicate key conflicts.
    """
    df_with_status = df.withColumn("lifecycle_status", map_lifecycle_status_expr())

    window_spec = Window.partitionBy("trade_key").orderBy(
        F.col("dissemination_identifier").desc()
    )

    return (
        df_with_status.withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def create_gold_table_if_not_exists(spark, gold_table: str) -> None:
    """Create the Gold Iceberg table if it does not already exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_table} (
            trade_key STRING,
            current_dissemination_id STRING,
            action_type STRING,
            lifecycle_status STRING,
            event_timestamp TIMESTAMP,
            execution_timestamp TIMESTAMP,
            notional_amount_leg_1 DOUBLE,
            is_capped_notional_leg_1 BOOLEAN,
            notional_amount_leg_2 DOUBLE,
            is_capped_notional_leg_2 BOOLEAN,
            fixed_rate_leg_1 DOUBLE,
            effective_date STRING,
            expiration_date STRING,
            asset_class STRING,
            _last_file_date STRING,
            _updated_at TIMESTAMP,
            version INT
        ) USING iceberg
    """)


def merge_into_gold(spark, silver_table: str, gold_table: str) -> None:
    """Execute Iceberg SQL MERGE INTO to reconcile latest trade states directly from Silver."""
    create_gold_table_if_not_exists(spark, gold_table)

    # Purge any invalid null trade_key rows
    spark.sql(f"DELETE FROM {gold_table} WHERE trade_key IS NULL")

    # Use pure Iceberg SQL subquery to allow Catalyst to plan the v2 Iceberg source natively
    merge_sql = f"""
        MERGE INTO {gold_table} AS target
        USING (
            SELECT * FROM (
                SELECT 
                    trade_key,
                    dissemination_identifier,
                    action_type,
                    CASE 
                        WHEN action_type = 'EROR' THEN 'CANCELLED'
                        WHEN action_type = 'TERM' THEN 'TERMINATED'
                        WHEN action_type IN ('NEWT', 'MODI', 'CORR', 'REVI') THEN 'ACTIVE'
                        ELSE 'UNKNOWN'
                    END AS lifecycle_status,
                    event_timestamp,
                    execution_timestamp,
                    notional_amount_leg_1,
                    is_capped_notional_leg_1,
                    notional_amount_leg_2,
                    is_capped_notional_leg_2,
                    fixed_rate_leg_1,
                    effective_date,
                    expiration_date,
                    asset_class,
                    file_date,
                    row_number() OVER (
                        PARTITION BY trade_key 
                        ORDER BY dissemination_identifier DESC
                    ) as rn
                FROM {silver_table}
                WHERE trade_key IS NOT NULL AND trade_key != ''
            ) WHERE rn = 1
        ) AS source
        ON target.trade_key = source.trade_key
        WHEN MATCHED THEN
            UPDATE SET
                target.current_dissemination_id = source.dissemination_identifier,
                target.action_type = source.action_type,
                target.lifecycle_status = source.lifecycle_status,
                target.event_timestamp = source.event_timestamp,
                target.execution_timestamp = source.execution_timestamp,
                target.notional_amount_leg_1 = source.notional_amount_leg_1,
                target.is_capped_notional_leg_1 = source.is_capped_notional_leg_1,
                target.notional_amount_leg_2 = source.notional_amount_leg_2,
                target.is_capped_notional_leg_2 = source.is_capped_notional_leg_2,
                target.fixed_rate_leg_1 = source.fixed_rate_leg_1,
                target.effective_date = source.effective_date,
                target.expiration_date = source.expiration_date,
                target.asset_class = source.asset_class,
                target._last_file_date = source.file_date,
                target._updated_at = current_timestamp(),
                target.version = target.version + 1
        WHEN NOT MATCHED THEN
            INSERT (
                trade_key,
                current_dissemination_id,
                action_type,
                lifecycle_status,
                event_timestamp,
                execution_timestamp,
                notional_amount_leg_1,
                is_capped_notional_leg_1,
                notional_amount_leg_2,
                is_capped_notional_leg_2,
                fixed_rate_leg_1,
                effective_date,
                expiration_date,
                asset_class,
                _last_file_date,
                _updated_at,
                version
            )
            VALUES (
                source.trade_key,
                source.dissemination_identifier,
                source.action_type,
                source.lifecycle_status,
                source.event_timestamp,
                source.execution_timestamp,
                source.notional_amount_leg_1,
                source.is_capped_notional_leg_1,
                source.notional_amount_leg_2,
                source.is_capped_notional_leg_2,
                source.fixed_rate_leg_1,
                source.effective_date,
                source.expiration_date,
                source.asset_class,
                source.file_date,
                current_timestamp(),
                1
            )
    """
    print(f"Executing MERGE INTO on {gold_table}...")
    spark.sql(merge_sql)
    print("MERGE INTO operation completed.")


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    silver_table = f"{catalog}.dtcc.silver_rates"
    gold_table = f"{catalog}.dtcc.gold_active_trades"

    print(f"\n--- Starting Trade Corrections Engine: Silver ({silver_table}) -> Gold ({gold_table}) ---")
    spark = get_spark_session(
        app_name=f"DTCC-Gold-ApplyCorrections-{catalog}",
        enable_iceberg=True,
        enable_polaris=(catalog == "polaris"),
    )

    # 1. Read cleaned Silver events from Iceberg table
    print(f"1. Reading Silver events from {silver_table}...")
    df_silver = spark.table(silver_table)
    total_silver_events = df_silver.count()
    print(f"Loaded {total_silver_events:,} Silver events.")

    # 2. Compute unique trade keys count for verification
    print("2. Deduplicating to latest event per trade_key...")
    unique_trades_count = (
        df_silver.filter(F.col("trade_key").isNotNull() & (F.col("trade_key") != ""))
        .select("trade_key")
        .distinct()
        .count()
    )
    print(f"Found {unique_trades_count:,} unique valid trade keys across Silver.")

    # 3. Apply MERGE INTO against Gold Iceberg table
    print(f"3. Applying MERGE INTO against {gold_table}...")
    merge_into_gold(spark, silver_table, gold_table)

    # 4. Verify Gold table record count
    print(f"\n4. Verifying Gold table '{gold_table}'...")
    df_gold = spark.table(gold_table)
    gold_count = df_gold.count()
    print(f"Total trades in Gold table: {gold_count:,}")
    assert gold_count == unique_trades_count, (
        f"Mismatch! Expected unique keys: {unique_trades_count}, Gold rows: {gold_count}"
    )
    print(f"Verified: Gold table has exactly 1 row per unique trade_key ({gold_count:,} rows).")

    # 5. Lifecycle breakdown in Gold table
    print("\n5. Lifecycle status distribution in Gold:")
    df_gold.groupBy("lifecycle_status").count().orderBy("count", ascending=False).show(truncate=False)

    # 6. Sample Gold records preview
    print("6. Sample Gold records preview:")
    df_gold.select(
        "trade_key",
        "lifecycle_status",
        "action_type",
        "version",
        "notional_amount_leg_1",
        "is_capped_notional_leg_1",
        "_updated_at",
    ).show(5, truncate=False)

    # 7. Inspect Iceberg Snapshot Metadata for Gold
    print("7. Querying Gold Iceberg Snapshots Metadata:")
    spark.sql(f"""
        SELECT 
            snapshot_id, 
            parent_id, 
            operation, 
            summary['total-records'] AS total_records,
            summary['added-records'] AS added_records,
            committed_at
        FROM {gold_table}.snapshots
    """).show(truncate=False)

    spark.stop()
    print(f"--- Trade Corrections Engine Finished Successfully ({gold_table}) ---\n")


if __name__ == "__main__":
    main()
