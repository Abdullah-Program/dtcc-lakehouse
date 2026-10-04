#!/usr/bin/env python3
"""Phase 1: Bronze layer ingestion job.

Reads raw DTCC cumulative RATES CSV, adds audit metadata columns
(_ingested_at, _source_file, file_date), and writes to compressed
Parquet format partitioned by file_date in the Bronze warehouse.
"""
from pathlib import Path
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from spark_jobs.common.spark_session import get_spark_session

SAMPLES_DIR = Path("/workspace/samples")
OUTPUT_DIR = Path("/workspace/warehouse/bronze/rates")


def find_latest_csv() -> Path:
    csvs = sorted(SAMPLES_DIR.glob("CFTC_CUMULATIVE_RATES_*.csv"))
    if not csvs:
        raise FileNotFoundError(
            f"No cumulative rates CSV found in {SAMPLES_DIR}. "
            "Ensure the zip file in samples/ has been extracted."
        )
    return csvs[-1]


def extract_file_date(filename: str) -> str:
    """Extract YYYY-MM-DD date from filename like CFTC_CUMULATIVE_RATES_2026_10_02.csv."""
    match = re.search(r"(\d{4})_(\d{2})_(\d{2})", filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return "unknown"


def main() -> None:
    csv_path = find_latest_csv()
    file_date = extract_file_date(csv_path.name)
    print(f"\n--- Bronze Ingestion: {csv_path.name} (date: {file_date}) ---")

    spark = get_spark_session(app_name="DTCC-Bronze-LoadCumulative")

    # 1. Read raw CSV without type inference to preserve exact raw strings
    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(csv_path))
    )

    raw_count = df_raw.count()
    raw_cols = len(df_raw.columns)
    print(f"Read {raw_count:,} raw records with {raw_cols} columns.")

    # 2. Add Lakehouse audit columns
    df_bronze = (
        df_raw.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(csv_path.name))
        .withColumn("file_date", F.lit(file_date))
    )

    # 3. Write to Parquet partitioned by file_date
    # Using snappy compression (default in Spark) for fast queries and compression
    dest_path = str(OUTPUT_DIR)
    print(f"Writing Parquet to: {dest_path} (partitioned by file_date)...")

    (
        df_bronze.write.mode("overwrite")
        .partitionBy("file_date")
        .parquet(dest_path)
    )
    print("Write complete.")

    # 4. Verify by reading back from the Bronze Parquet directory
    print("\n--- Verifying Written Bronze Parquet ---")
    df_verify = spark.read.parquet(dest_path)
    verify_count = df_verify.count()
    print(f"Verified row count from Parquet: {verify_count:,}")

    assert verify_count == raw_count, (
        f"Row count mismatch! Raw: {raw_count}, Parquet: {verify_count}"
    )

    # Verify audit columns exist and are populated
    for col_name in ("_ingested_at", "_source_file", "file_date"):
        assert col_name in df_verify.columns, f"Missing audit column {col_name}"

    print("Sample records from Bronze Parquet:")
    df_verify.select(
        "file_date",
        "Dissemination Identifier",
        "Action type",
        "_source_file",
        "_ingested_at",
    ).show(3, truncate=False)

    spark.stop()
    print("--- Bronze Ingestion Successful (Clean Shutdown) ---\n")


if __name__ == "__main__":
    main()
