#!/usr/bin/env python3
"""Phase 1: Ingest and validate raw DTCC cumulative RATES CSV using PySpark.

Reads the raw CSV with:
- header=True
- inferSchema=False (Bronze principle: maintain raw fidelity, all fields as String)
- Validates key columns, row counts, and Action type distribution.
"""
from pathlib import Path
from pyspark.sql.types import StringType
from spark_jobs.common.spark_session import get_spark_session

SAMPLES_DIR = Path("/workspace/samples")
KEY_COLUMNS = [
    "Dissemination Identifier",
    "Original Dissemination Identifier",
    "Action type",
    "Event type",
    "Event timestamp",
]


def find_latest_csv() -> Path:
    csvs = sorted(SAMPLES_DIR.glob("CFTC_CUMULATIVE_RATES_*.csv"))
    if not csvs:
        raise FileNotFoundError(
            f"No cumulative rates CSV found in {SAMPLES_DIR}. "
            "Ensure the zip file in samples/ has been extracted."
        )
    return csvs[-1]


def main() -> None:
    csv_path = find_latest_csv()
    print(f"\n--- Reading Raw DTCC File: {csv_path.name} ---")

    spark = get_spark_session(app_name="DTCC-Bronze-ReadRawRates")

    # Ingest CSV without schema inference to preserve raw strings (especially IDs)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(csv_path))
    )

    total_rows = df.count()
    total_cols = len(df.columns)
    print(f"Loaded DataFrame: {total_rows:,} rows, {total_cols} columns")

    # 1. Validate key columns exist
    missing = [c for c in KEY_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected key columns: {missing}")
    print(f"Verified presence of all key columns: {KEY_COLUMNS}")

    # 2. Verify Dissemination IDs are treated as Strings
    id_field = df.schema["Dissemination Identifier"]
    orig_id_field = df.schema["Original Dissemination Identifier"]
    assert isinstance(id_field.dataType, StringType), "ID must be StringType"
    assert isinstance(orig_id_field.dataType, StringType), "Orig ID must be StringType"
    print("Verified: Dissemination IDs are stored as StringType (no precision loss).")

    # 3. Action type distribution (cross-verifies Phase 0 exploratory Python script)
    print("\n--- Action Type Distribution ---")
    action_counts = (
        df.groupBy("Action type")
        .count()
        .orderBy("count", ascending=False)
    )
    action_counts.show(truncate=False)

    # 4. Preview sample rows with key columns
    print("--- Sample Records Preview ---")
    df.select(KEY_COLUMNS).show(5, truncate=False)

    spark.stop()
    print("--- Raw DTCC Read & Validation Complete (Clean Shutdown) ---\n")


if __name__ == "__main__":
    main()
