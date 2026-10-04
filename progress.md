# Progress

## Current phase
Phase 1 (Spark basics): Step 1.4 complete (Bronze Parquet ingestion job load_cumulative.py writes partitioned raw data with audit metadata, verified 26,840 rows). Next: Silver cleaning job (clean_trades.py).

## Environment (one line)
Windows host, 15.7 GB RAM (about 12.7 GB already in use at idle), 24-thread CPU; WSL2 Ubuntu capped at 7.6 GiB; Python 3.14.4 in WSL; PySpark 4.1.3 in Docker; repo at D:\programining\dtcc-lakehouse (WSL: /mnt/d/programining/dtcc-lakehouse).

## What works
- Repo on GitHub (public), PR workflow with gh CLI, Issues #1, #3, #6, #8, #10, #12 closed via PRs.
- ingestion/inspect_dtcc.py runs against live DTCC; real schema is in docs/data-notes.md.
- ingestion/explore_actions.py and explore_chains.py analyse the cached CFTC RATES zip.
- Docker Compose PySpark service with 2500M RAM cap and spark_jobs/hello_spark.py smoke test verified.
- spark_jobs/common/spark_session.py factory with memory limits (1500M driver, local[*], 2 shuffle partitions).
- spark_jobs/bronze/read_raw_rates.py verifies 26,840 rows, 110 columns, and exact Action type counts.
- spark_jobs/bronze/load_cumulative.py writes Snappy Parquet to warehouse/bronze/rates/file_date=YYYY-MM-DD with _ingested_at and _source_file, verified 26,840 rows.

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
