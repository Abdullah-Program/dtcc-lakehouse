# Progress

## Current phase
Phase 4 (Streaming): Step 4.1 complete (Redpanda Kafka broker deployed in docker-compose.yml with 750M memory limit, ADR 0003 documented, topic dtcc.rates.raw created and verified). Next: Step 4.2 implement streaming producer (ingestion/kafka_producer.py) and PySpark Structured Streaming consumer into Iceberg (spark_jobs/streaming/kafka_to_iceberg.py).

## Environment (one line)
Windows host, 15.7 GB RAM (about 12.7 GB already in use at idle), 24-thread CPU; WSL2 Ubuntu capped at 7.6 GiB; Python 3.14.4 in WSL; PySpark 4.1.3 in Docker; repo at D:\programining\dtcc-lakehouse (WSL: /mnt/d/programining/dtcc-lakehouse).

## What works
- Repo on GitHub (public), PR workflow with gh CLI, Issues #1, #3, #6, #8, #10, #12, #14, #16, #18, #20, #22, #24, #26 closed via PRs.
- ingestion/inspect_dtcc.py runs against live DTCC; real schema is in docs/data-notes.md.
- ingestion/explore_actions.py and explore_chains.py analyse the cached CFTC RATES zip.
- Docker Compose PySpark service with 2500M RAM cap and spark_jobs/hello_spark.py smoke test verified.
- spark_jobs/common/spark_session.py factory with memory limits (1500M driver, local[*], 2 shuffle partitions).
- spark_jobs/bronze/read_raw_rates.py verifies 26,840 rows, 110 columns, and exact Action type counts.
- spark_jobs/bronze/load_cumulative.py writes Snappy Parquet to warehouse/bronze/rates/file_date=YYYY-MM-DD with _ingested_at and _source_file, verified 26,840 rows.
- spark_jobs/silver/clean_trades.py transforms Bronze to Silver Parquet: snake_case column names, trade_key derivation, TimestampType parsing, notional comma/cap sanitization, verified 26,840 rows.
- tests/test_silver_clean.py verified via unittest inside container.
- spark_jobs/iceberg_smoke_test.py verifies Iceberg 1.11.0 integration, extensions, local catalog, table creation, and snapshot metadata inspection.
- spark_jobs/silver/load_silver_iceberg.py loads Silver Parquet into local.dtcc.silver_rates Iceberg table partitioned by file_date; verifies 26,840 rows, snapshots, and data files.
- spark_jobs/gold/apply_corrections.py reconciles trade lifecycles (NEWT/MODI/CORR -> ACTIVE, TERM -> TERMINATED, EROR -> CANCELLED) via Iceberg MERGE INTO with snapshot versioning.
- spark_jobs/gold/time_travel_demo.py demonstrates Iceberg zero-copy time travel across historical commits.
- tests/test_corrections.py unit tests verified (lifecycle mapping and window deduplication).
- Redpanda broker running with 750M limit, verified cluster health and topic dtcc.rates.raw via rpk; docs/adr/0003-redpanda-for-kafka.md documented.

## Key findings (CFTC RATES, 2026-10-02, 26,840 rows, 110 columns)
- 6 Action types; ~20% of rows are not NEWT.
- Original ID empty only for NEWT; same-file targets are NEWT rows (root trade).
- ~32% of non-NEWT rows point to a trade outside this file, so history is needed.
- Provisional trade_key: own ID for NEWT, Original ID otherwise (re-check on a month of data).

## Stuck / open
- Official meaning of action types and Amendment indicator (read DTCC/CFTC docs before Phase 3).
- Docker Desktop WSL integration tested and working cleanly.

## Revise next time
- Run one command block, check output, then continue.
- Replace every <placeholder> with a real value.
