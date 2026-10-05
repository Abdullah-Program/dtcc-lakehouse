# Progress

## Current phase
Phase 6 (Trino Query Engine & Table Maintenance Lab): Steps 6.1, 6.2, and 6.3 verified (Tracking Issue #40 on branch feature/phase6-trino-maintenance-lab).
- Completed:
  1. Deployed Trino 483 in Docker Compose (`dtcc_trino`) with strict memory cap (1500M container limit, `-Xmx1024M` JVM heap, G1GC tuning); Web UI active on `http://localhost:8080`.
  2. Integrated Trino with Apache Polaris REST Catalog and Garage S3 (`infra/trino/catalog/polaris.properties`); verified discovery of `dtcc.silver_rates`, `dtcc.gold_active_trades`, and `dtcc.smoke_rates`.
  3. Executed financial analytics SQL queries via Trino (`sql/trino/`): active notional risk by asset class ($100B+ across 21,476 IR trades), currency distribution, snapshot metadata audits, and zero-copy time travel (`FOR VERSION AS OF`).
  4. Implemented Table Maintenance Lab:
     - `spark_jobs/maintenance/compact_files.py`: Cut small data files by 50% (6 -> 3 files) and increased average file size by 86.5% on `silver_rates`.
     - `spark_jobs/maintenance/expire_snapshots.py`: Purged 4 stale snapshots, 7 manifests, 4 manifest lists, and 4 obsolete data files on `gold_active_trades` (retained last 2).
     - `spark_jobs/maintenance/remove_orphans.py` & `sql/trino/04_maintenance.sql`: Audited 22 storage objects on Garage S3 with a 7-day retention safety threshold.
  5. Documented ADR 0005 (`docs/adr/0005-trino-for-interactive-query-engine.md`) and comprehensive benchmark report (`docs/maintenance-lab.md`).
- Next: Open PR for Issue #40, merge into main, and transition to Phase 7 (CI/CD, Monitoring & Ops).

## Environment (one line)
Windows host, 15.7 GB RAM (about 12.7 GB already in use at idle), 24-thread CPU; WSL2 Ubuntu capped at 7.6 GiB; Python 3.14.4 in WSL; PySpark 4.1.3 in Docker; repo at D:\programining\dtcc-lakehouse (WSL: /mnt/d/programining/dtcc-lakehouse).

## What works
- Repo on GitHub (public), PR workflow with gh CLI, Issues #1, #3, #6, #8, #10, #12, #14, #16, #18, #20, #22, #24, #26, #28, #30, #32, #34 closed via PRs.
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
- ingestion/kafka_producer.py streams live JSON trade events into Redpanda topic dtcc.rates.raw via stdlib rpk pipe.
- spark_jobs/streaming/kafka_to_iceberg.py consumes Redpanda stream with PySpark Structured Streaming, aligns schema to 117-column Silver table, and commits ACID append snapshots to local.dtcc.silver_rates.
- Garage S3 object store running in Docker with 250M limit, single-node layout active, dtcc-lakehouse bucket provisioned with dtcc-key credentials, verified via tests/test_garage_s3.py (PUT/GET/LIST); docs/adr/0002-garage-instead-of-minio.md documented.
- Apache Polaris Iceberg REST catalog running in Docker with 512M limit, dtcc_catalog mapped to s3://dtcc-lakehouse/, PySpark dual-catalog integration (local Hadoop + polaris REST with S3FileIO), verified via spark_jobs/polaris_smoke_test.py; docs/adr/0004-polaris-for-iceberg-rest-catalog.md documented.

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
