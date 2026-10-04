#!/usr/bin/env python3
"""Phase 1: Silver layer cleaning and standardization job.

Reads Bronze Parquet from /workspace/warehouse/bronze/rates,
standardizes column names to snake_case, derives trade_key for corrections linking,
casts timestamps and numerical amounts, and writes to
/workspace/warehouse/silver/rates partitioned by file_date.
"""
from pathlib import Path
import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark_jobs.common.spark_session import get_spark_session

BRONZE_DIR = Path("/workspace/warehouse/bronze/rates")
SILVER_DIR = Path("/workspace/warehouse/silver/rates")


def sanitize_column_name(name: str) -> str:
    """Convert raw header to clean lowercase snake_case.

    Replaces spaces, hyphens, slashes, and special characters with single underscores.
    Example:
        'Dissemination Identifier' -> 'dissemination_identifier'
        'Notional amount-Leg 1'    -> 'notional_amount_leg_1'
        'Event timestamp'          -> 'event_timestamp'
    """
    clean = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip())
    return clean.strip("_").lower()


def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename all DataFrame columns to clean snake_case."""
    for col_name in df.columns:
        clean_name = sanitize_column_name(col_name)
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    return df


def add_trade_key(df: DataFrame) -> DataFrame:
    """Derive trade_key for lifecycle linking across mutations.

    Rule:
    - If original_dissemination_identifier exists and is non-empty, trade_key = original ID.
    - Otherwise (e.g. root NEWT trade), trade_key = own dissemination_identifier.
    """
    orig_col = F.col("original_dissemination_identifier")
    own_col = F.col("dissemination_identifier")

    is_valid_orig = (
        orig_col.isNotNull()
        & (F.trim(orig_col) != "")
        & (F.upper(F.trim(orig_col)) != "NULL")
    )

    return df.withColumn(
        "trade_key",
        F.when(is_valid_orig, F.trim(orig_col)).otherwise(F.trim(own_col)),
    )


def cast_types(df: DataFrame) -> DataFrame:
    """Cast timestamp strings and numerical amounts to native Spark types.

    Preserves dissemination IDs and trade_key strictly as StringType.
    Sanitizes formatted numbers (removes commas, handles regulatory '+' cap flags).
    """
    # 1. Timestamps (ISO-8601 strings like '2026-10-02T21:59:05Z')
    if "event_timestamp" in df.columns:
        df = df.withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp")),
        )
    if "execution_timestamp" in df.columns:
        df = df.withColumn(
            "execution_timestamp",
            F.to_timestamp(F.col("execution_timestamp")),
        )

    # 2. Financial numerics with sanitization
    # DTCC notional amounts contain commas (e.g. '50,000,000') and regulatory cap markers
    # '+' (e.g. '470,000,000+' for trades exceeding CFTC public reporting caps).
    if "notional_amount_leg_1" in df.columns:
        df = df.withColumn(
            "is_capped_notional_leg_1",
            F.col("notional_amount_leg_1").endswith("+"),
        ).withColumn(
            "notional_amount_leg_1",
            F.expr("try_cast(regexp_replace(trim(notional_amount_leg_1), '[,+]', '') as double)"),
        )

    if "notional_amount_leg_2" in df.columns:
        df = df.withColumn(
            "is_capped_notional_leg_2",
            F.col("notional_amount_leg_2").endswith("+"),
        ).withColumn(
            "notional_amount_leg_2",
            F.expr("try_cast(regexp_replace(trim(notional_amount_leg_2), '[,+]', '') as double)"),
        )

    if "fixed_rate_leg_1" in df.columns:
        df = df.withColumn(
            "fixed_rate_leg_1",
            F.expr("try_cast(regexp_replace(trim(fixed_rate_leg_1), '[,%]', '') as double)"),
        )

    return df


def clean_silver(df: DataFrame) -> DataFrame:
    """Apply the full Silver cleaning pipeline to Bronze DataFrame."""
    df_clean = standardize_columns(df)
    df_keyed = add_trade_key(df_clean)
    df_typed = cast_types(df_keyed)
    df_silver = df_typed.withColumn("_cleaned_at", F.current_timestamp())
    return df_silver


def main() -> None:
    print(f"\n--- Reading Bronze Parquet from: {BRONZE_DIR} ---")
    spark = get_spark_session(app_name="DTCC-Silver-CleanTrades")

    df_bronze = spark.read.parquet(str(BRONZE_DIR))
    bronze_count = df_bronze.count()
    print(f"Loaded {bronze_count:,} Bronze records.")

    # Apply Silver transformations
    df_silver = clean_silver(df_bronze)

    # Write to Silver Parquet partitioned by file_date
    dest_path = str(SILVER_DIR)
    print(f"Writing Cleaned Silver Parquet to: {dest_path}...")
    (
        df_silver.write.mode("overwrite")
        .partitionBy("file_date")
        .parquet(dest_path)
    )
    print("Write complete.")

    # Verification: Read back from Silver Parquet
    print("\n--- Verifying Written Silver Parquet ---")
    df_verify = spark.read.parquet(dest_path)
    verify_count = df_verify.count()
    print(f"Verified row count from Silver Parquet: {verify_count:,}")
    assert verify_count == bronze_count, (
        f"Row count mismatch! Bronze: {bronze_count}, Silver: {verify_count}"
    )

    # Verify trade_key is populated for 100% of rows
    null_keys = df_verify.filter(F.col("trade_key").isNull()).count()
    assert null_keys == 0, f"Found {null_keys} rows with null trade_key!"
    print("Verified: 100% of rows have a valid non-null trade_key.")

    # Verify event_timestamp is TimestampType
    ts_type = df_verify.schema["event_timestamp"].dataType
    assert isinstance(ts_type, T.TimestampType), (
        f"Expected TimestampType for event_timestamp, got {ts_type}"
    )
    print("Verified: event_timestamp successfully cast to native TimestampType.")

    # Preview sample cleaned records with their derived trade_key
    print("\n--- Sample Cleaned Silver Records ---")
    df_verify.select(
        "file_date",
        "trade_key",
        "dissemination_identifier",
        "original_dissemination_identifier",
        "action_type",
        "event_timestamp",
        "notional_amount_leg_1",
        "_cleaned_at",
    ).show(5, truncate=False)

    spark.stop()
    print("--- Silver Cleaning Job Complete (Clean Shutdown) ---\n")


if __name__ == "__main__":
    main()
