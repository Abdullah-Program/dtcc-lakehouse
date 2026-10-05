# DTCC Open Data Lakehouse: context for AI assistants

## 1. What this project is
A learning and portfolio project, built from scratch by a beginner in Spark/Iceberg/streaming.
Goal: look like real work for an "AWS Data Engineer - Open Data Platform" role (AWS Glue, Spark, S3,
Apache Iceberg, Apache Polaris + Iceberg REST catalog, Snowflake, Databricks, CI/CD, benchmarking,
table maintenance, monitoring). Everything must run free, on a laptop.

Flagship idea: a trade corrections engine. DTCC public swap price messages (new / correct / cancel / ...)
are applied to Iceberg tables with MERGE INTO, time travel is used for audit, plus an Iceberg
maintenance lab (small files, compaction, expire_snapshots, orphan file cleanup) with before/after numbers.

## 2. Free stack (do not substitute without asking)
PySpark in Docker, Apache Iceberg, Redpanda (Kafka API), Apache Polaris, Garage for S3 (or local filesystem),
Trino as a second engine, Prometheus + Grafana, GitHub Actions. Snowflake trial and Databricks Free Edition
only in the last phase.
- MinIO is archived: do NOT use it. Use Garage.
- Chosen Spark image (verified 2026-10-04): apache/spark:4.1.3-scala2.13-java17-python3-r-ubuntu
- Planned Iceberg: 1.12.0 with the Spark 4.1 / Scala 2.13 runtime jar (verify the exact artifact name
  before using it; 1.12.0 is very new, 1.11.0 is the fallback).
- Older local images (tabulario/spark-iceberg etc.) are NOT used.

## 3. Roadmap (follow in order, always say which phase we are in)
0 Setup | 1 Spark basics | 2 Iceberg | 3 Corrections logic | 4 Streaming |
5 Storage + Polaris | 6 Trino + maintenance lab | 7 CI/CD + ops | 8 Snowflake/Databricks + write-up

## 4. Current status
- Phase 0 done. PRs #2, #4, #7, #9, #11, #13, #15, #17, #19, #21, #23, #25, #27, #29, #31, #33 merged to main. Repo is public on GitHub: Abdullah-Program/dtcc-lakehouse.
- Phase 1 (Spark basics) complete: raw CSV validation, Bronze Parquet, and Silver cleaning jobs verified.
- Phase 2 (Iceberg) complete: Iceberg 1.11.0 runtime, extensions, local catalog, and Silver Iceberg table (local.dtcc.silver_rates) verified.
- Phase 3 (Trade Corrections Engine) complete: Iceberg SQL MERGE INTO, stateful lifecycle reconciliation (ACTIVE/TERMINATED/CANCELLED), snapshot versioning, and Gold table (local.dtcc.gold_active_trades) verified.
- Phase 4 (Streaming) complete: Redpanda Kafka broker deployed, lightweight stdlib Kafka producer (ingestion/kafka_producer.py), and PySpark Structured Streaming consumer (spark_jobs/streaming/kafka_to_iceberg.py) appending micro-batches into local.dtcc.silver_rates with snapshot lineage verified.
- Phase 5 (Storage & Catalog Decoupling): Step 5.1 complete (Garage S3 object store deployed with 250M limit, single-node layout configured, dtcc-lakehouse bucket provisioned with dtcc-key credentials, verified via tests/test_garage_s3.py, ADR 0002 documented).
- Step 5.2 complete (Apache Polaris REST Catalog deployed with 512M limit, dtcc_catalog mapped to s3://dtcc-lakehouse/, PySpark dual-catalog configuration local Hadoop + polaris REST with S3FileIO verified via spark_jobs/polaris_smoke_test.py, ADR 0004 documented).
- Step 5.3 complete (Migrated Silver and Gold Iceberg tables and PySpark Structured Streaming pipeline to Garage S3 + Polaris REST catalog; verified micro-batch append to polaris.dtcc.silver_rates and MERGE INTO on polaris.dtcc.gold_active_trades).
- Next: Create PR for Issue #38 and advance to Phase 6 (Trino Query Engine & Table Maintenance Lab).

## 5. Environment
- Windows host with 15.7 GB RAM (often 80% in use), 24-thread CPU. WSL2 Ubuntu is capped at about 7.6 GiB.
- Docker Desktop with WSL integration (do not install docker.io inside WSL; never run `docker system prune -a`).
- Repo path: D:\programining\dtcc-lakehouse = /mnt/d/programining/dtcc-lakehouse in WSL.
- Run ALL git and Linux commands in WSL bash, never PowerShell. `wsl` commands are Windows-only (use `wsl.exe` from bash).
- WSL has Python 3.14.4 for small stdlib scripts. Spark always runs in the Docker container, not on the host.
- RAM is the bottleneck: run only the services the current phase needs, always set container memory limits,
  and ask before adding a service.

## 6. DTCC data: verified facts only
- Bucket name is resolved at runtime from https://pddata.dtcc.com/ppd/api/general/bucketname (never hardcode it).
  On 2026-10-04 it returned kgc0418-tdw-data-0, region "prod". Files live at https://<bucket>.s3.amazonaws.com/.
- dashboard/Ticker.json, Slice.json, Cumulative.json are dicts keyed by feed, e.g. CA_CR, CA_EQ, CA_IR,
  CFTC_CO, CFTC_CR, CFTC_EQ, CFTC_FX, CFTC_IR (11 keys, 3 not yet inspected).
- Ticker.json items use camelCase (actionType, disseminationIdentifier, disseminationTimestamp, eventTimestamp...).
  The CSV uses spaced headers ("Action type", "Dissemination Identifier"). A mapping is needed for streaming.
- Slice.json lists small zip slices (about every 10 s) with fileName, fullFilePath, rowCount, sliceId, startTs, endTs.
- Daily file: cftc/eod/CFTC_CUMULATIVE_RATES_YYYY_MM_DD.zip. The 2026-10-02 file has 26,840 rows and 110 columns.
- SCOPE for now: CFTC RATES only.
- NEVER invent column names. The source of truth is the header row of the cached zip in samples/
  (samples/ is gitignored) and docs/data-notes.md.
- Key columns: "Dissemination Identifier" (id of one message, unique per row, always 19 digits),
  "Original Dissemination Identifier", "Action type", "Event type", "Event timestamp", "Amendment indicator".
- Action type values seen: NEWT, MODI, CORR, TERM, EROR, REVI. Their official meaning is NOT yet verified:
  read DTCC/CFTC documentation before Phase 3 and never guess.
- Findings from one day of data (re-check on a month in Phase 1):
  * about 20% of rows are not NEWT.
  * Original ID is empty only for NEWT; same-file targets are NEWT rows (root trade), not the previous message.
  * about 32% of non-NEWT rows point to a trade outside that day's file, so history is required.
  * Child id is always larger than the id it points to (candidate ordering key).
  * "Event type" is empty exactly for CORR/EROR/REVI.
  * 33 self-referencing rows exist (own id == original id).
  * IDs: some Original IDs are 8-10 or 18 digits. ALWAYS treat IDs as strings, never float/int.
  * "Event timestamp" can be the sentinel 2000-01-01T00:00:00Z.
  * The CSV has no dissemination timestamp column, so ordering needs another key.
- Provisional design (not final): trade_key = own id for NEWT, Original ID otherwise.
  Silver = one row per message (append-only). Gold = latest state per trade_key.
- Be polite to DTCC: cache files locally under samples/, no aggressive polling, no loops hitting the servers,
  keep the descriptive User-Agent already used in ingestion/inspect_dtcc.py.

## 7. Repo structure
README.md, progress.md, Makefile, pyproject.toml, requirements.txt, docker-compose.yml, .env.example
docs/ (architecture.md, runbook.md, data-notes.md, adr/)
ingestion/ (inspect_dtcc.py, explore_actions.py, explore_chains.py, download_cumulative.py, poll_ticker.py, kafka_producer.py)
spark_jobs/ (common/, bronze/, silver/, gold/, streaming/, maintenance/)
sql/ (trino/, snowflake/) | tests/ | infra/ (polaris, garage, trino, terraform) | orchestration/
monitoring/ (prometheus, grafana) | benchmarks/ | samples/ (gitignored) | warehouse/ (gitignored) | .github/workflows/
Many files are still empty placeholders on purpose; fill them only when their phase arrives.

## 8. Working rules for the AI assistant
- The user is learning from scratch. Make ONE small change at a time. Before code, explain why in 2-3 lines.
  After code, say how to verify it and what commonly breaks. Explain any line the user does not understand.
- Always state the exact file path to create or edit. Never use `<placeholders>` in commands; use real values or ask.
- Do not guess fast-changing facts (versions, DTCC endpoints, Polaris/Iceberg configs, free-tier limits).
  Say "verify" or search the official docs.
- Never push to main. Workflow: GitHub Issue -> branch (feature/..., chore/...) -> small commits ->
  PR with "Closes #N" -> squash merge. Conventional commit messages (feat:, fix:, docs:, chore:).
- Never commit secrets, tokens, or .env. Never commit samples/ or warehouse/.
- Add tests (pytest) for transformations, lint with ruff, keep functions small.
- Be honest: if an approach is too heavy for this laptop or wrong, say so and propose something lighter.
- Reply in simple, concise English.
