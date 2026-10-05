#!/usr/bin/env python3
"""Phase 4: Spark Structured Streaming from Kafka (Redpanda) to Apache Iceberg.

Consumes real-time trade messages from Redpanda topic 'dtcc.rates.raw',
parses JSON payloads, sanitizes notional amounts, types timestamps,
aligns schema against target Iceberg Silver table, and appends micro-batches.
"""
import argparse
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark_jobs.common.spark_session import get_spark_session

KAFKA_BOOTSTRAP = "redpanda:29092"
TOPIC_NAME = "dtcc.rates.raw"


def parse_args():
    parser = argparse.ArgumentParser(description="Streaming from Kafka to Apache Iceberg")
    parser.add_argument(
        "--catalog",
        choices=["polaris", "local"],
        default="polaris",
        help="Iceberg catalog to target (polaris REST or local Hadoop)",
    )
    return parser.parse_args()


# Schema for incoming JSON payloads published by kafka_producer.py
TICKER_PAYLOAD_SCHEMA = T.StructType([
    T.StructField("trade_key", T.StringType(), False),
    T.StructField("dissemination_identifier", T.StringType(), False),
    T.StructField("original_dissemination_identifier", T.StringType(), True),
    T.StructField("action_type", T.StringType(), True),
    T.StructField("event_type", T.StringType(), True),
    T.StructField("event_timestamp", T.StringType(), True),
    T.StructField("execution_timestamp", T.StringType(), True),
    T.StructField("asset_class", T.StringType(), True),
    T.StructField("effective_date", T.StringType(), True),
    T.StructField("expiration_date", T.StringType(), True),
    T.StructField("notional_amount_leg_1", T.StringType(), True),
    T.StructField("notional_amount_leg_2", T.StringType(), True),
    T.StructField("file_date", T.StringType(), True),
])


def clean_streaming_events(df: DataFrame) -> DataFrame:
    """Cast timestamps, date, and sanitize financial numerics from JSON payload."""
    return (
        df.withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("execution_timestamp", F.to_timestamp(F.col("execution_timestamp")))
        .withColumn("file_date", F.to_date(F.col("file_date")))
        .withColumn(
            "is_capped_notional_leg_1",
            F.coalesce(F.col("notional_amount_leg_1").endswith("+"), F.lit(False)),
        )
        .withColumn(
            "notional_amount_leg_1",
            F.expr("try_cast(regexp_replace(trim(notional_amount_leg_1), '[,+]', '') as double)"),
        )
        .withColumn(
            "is_capped_notional_leg_2",
            F.coalesce(F.col("notional_amount_leg_2").endswith("+"), F.lit(False)),
        )
        .withColumn(
            "notional_amount_leg_2",
            F.expr("try_cast(regexp_replace(trim(notional_amount_leg_2), '[,+]', '') as double)"),
        )
        .withColumn("_cleaned_at", F.current_timestamp())
    )


def make_micro_batch_processor(target_table: str):
    """Factory creating micro-batch processor targeting a specific Iceberg table."""
    def process_micro_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        batch_count = batch_df.count()
        print(f"Processing micro-batch {batch_id} with {batch_count:,} streaming events...")

        spark = batch_df.sparkSession
        # Align incoming streaming columns to the full 117-column Iceberg Silver table schema
        target_empty_df = spark.table(target_table).limit(0)
        aligned_df = target_empty_df.unionByName(batch_df, allowMissingColumns=True)

        # Append aligned micro-batch to Iceberg table with ACID transaction
        aligned_df.writeTo(target_table).append()
        print(f"Micro-batch {batch_id} appended successfully into {target_table}.")

    return process_micro_batch


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    target_table = f"{catalog}.dtcc.silver_rates"
    checkpoint_dir = f"/workspace/warehouse/checkpoints/streaming_silver_{catalog}"

    print(f"\n--- Starting Structured Streaming: Redpanda ({TOPIC_NAME}) -> Iceberg ({target_table}) ---")
    spark = get_spark_session(
        app_name=f"DTCC-Streaming-KafkaToIceberg-{catalog}",
        enable_iceberg=True,
        enable_kafka=True,
        enable_polaris=(catalog == "polaris"),
    )

    # Record initial table row count before streaming
    initial_count = spark.table(target_table).count()
    print(f"Pre-streaming row count in {target_table}: {initial_count:,}")

    # 1. Read Stream from Kafka / Redpanda
    print(f"1. Connecting to Redpanda at {KAFKA_BOOTSTRAP}, subscribing to '{TOPIC_NAME}'...")
    df_kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Deserialize JSON payload from Kafka 'value' binary column
    print("2. Parsing and transforming incoming trade messages...")
    df_parsed = df_kafka_raw.select(
        F.from_json(F.col("value").cast("string"), TICKER_PAYLOAD_SCHEMA).alias("payload")
    ).select("payload.*")

    df_stream_clean = clean_streaming_events(df_parsed)

    # 3. Stream micro-batches to Apache Iceberg table using availableNow trigger
    print(f"3. Streaming micro-batches to Iceberg table '{target_table}'...")
    process_batch = make_micro_batch_processor(target_table)
    query = (
        df_stream_clean.writeStream
        .foreachBatch(process_batch)
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print("Awaiting micro-batch streaming completion...")
    query.awaitTermination()
    print("Streaming micro-batch execution complete.")

    # 4. Verify post-streaming record count in Iceberg
    print(f"\n4. Verifying record count in '{target_table}'...")
    spark.catalog.refreshTable(target_table)
    final_count = spark.table(target_table).count()
    newly_added = final_count - initial_count
    print(f"Post-streaming row count : {final_count:,}")
    print(f"New trade records ingested : {newly_added:,}")

    # 5. Inspect latest snapshot in Iceberg table
    print("\n5. Querying latest Iceberg Snapshots:")
    spark.sql(f"""
        SELECT 
            snapshot_id, 
            parent_id, 
            operation, 
            summary['total-records'] AS total_records,
            summary['added-records'] AS added_records,
            committed_at
        FROM {target_table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 3
    """).show(truncate=False)

    spark.stop()
    print(f"--- Streaming Ingestion to Iceberg Succeeded Cleanly ({target_table}) ---\n")


if __name__ == "__main__":
    main()
