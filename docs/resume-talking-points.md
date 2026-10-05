# Data Engineering Resume & Interview Guide: DTCC Lakehouse

This guide provides battle-tested resume bullet points, architectural interview questions, and technical answers directly grounded in the DTCC Open Data Lakehouse codebase.

---

## 1. High-Impact Resume Bullet Points

### For Experience / Project Section:
- **Architected and deployed an enterprise-grade Financial Data Lakehouse** processing real-time OTC derivatives swap trades (Dodd-Frank Title VII) from DTCC using PySpark 4.1, Apache Iceberg, Redpanda, Garage S3, and Trino.
- **Engineered a stateful Trade Corrections Engine** utilizing Iceberg SQL `MERGE INTO`, reconciling 27,000+ mutating regulatory messages (NEWT, MODI, CORR, TERM, EROR) into a clean, audited Gold state table with ACID snapshot isolation.
- **Decoupled storage, metadata catalog, and query engines** by migrating from local Hadoop catalogs to an **Apache Polaris REST Catalog** with OAuth2 authentication and a containerized **Garage S3** object store.
- **Implemented real-time streaming ingestion** using Redpanda (Kafka API) and PySpark Structured Streaming, appending micro-batches directly to Iceberg with schema alignment and snapshot lineage.
- **Enabled sub-second interactive analytics** by integrating **Trino 483** over Polaris REST catalog, querying 21,000+ active contracts and $100B+ notional exposure without data movement.
- **Constructed an automated Table Maintenance Lab**: achieved a **50% reduction in file handles** via Iceberg bin-pack compaction (`rewrite_data_files`), pruned 4 stale snapshots, and automated orphan file sweeps in S3.
- **Built end-to-end CI/CD testing automation** using GitHub Actions, `ruff`, and `pytest`, enforcing data contracts and operational health across 5 containerized services.

---

## 2. Technical Interview Questions & Answers

### Q1: Why did you choose Apache Iceberg over Delta Lake or Apache Hudi?
> **Answer**:
> "We chose Apache Iceberg because it is an engine-agnostic open specification supported across Apache Spark, Trino, Snowflake, Flink, and DuckDB. Unlike Hive-style directory partitioning, Iceberg tracks data at the individual file level in an immutable metadata tree (Metadata -> Manifest List -> Manifest -> Data Files). This gives us three critical capabilities:
> 1. Hidden partitioning (partition evolution without rewriting historical datasets).
> 2. Safe, concurrent ACID `MERGE INTO` updates using optimistic concurrency control.
> 3. Zero-copy time travel (`FOR VERSION AS OF`), which is a hard requirement for regulatory audits under Dodd-Frank."

### Q2: Why did you deploy an Iceberg REST Catalog (Apache Polaris) instead of a Hive Metastore?
> **Answer**:
> "The legacy Hive Metastore (HMS) couples table metadata to a specific database backend and Thrift protocol, often consuming 1.5GB+ RAM and creating a single point of failure. Apache Polaris implements the vendor-neutral Iceberg REST OpenAPI specification. It provides:
> 1. Lightweight runtime (runs comfortably under 512MB RAM using Quarkus).
> 2. Centralized security with OAuth2 token exchange and role-based access control.
> 3. Engine interoperability: both Spark and Trino (and externally Snowflake) point to the exact same REST endpoint (`http://polaris:8181/api/catalog`) and discover tables automatically without manual schema synchronization."

### Q3: What is the 'Small Files Problem' in streaming, and how did your maintenance lab solve it?
> **Answer**:
> "Continuous streaming ingestion (e.g. 1-second micro-batches from Redpanda into Iceberg) writes dozens of small Parquet files. When query engines read a table with thousands of tiny files, query planning slows down dramatically due to S3 GET request latency and file opening overhead.
> In our Table Maintenance Lab:
> 1. We ran `CALL polaris.system.rewrite_data_files` using the `binpack` strategy, which combined small micro-batch files into balanced Parquet files, cutting our file count by 50% and doubling average file size with zero downtime.
> 2. We paired compaction with `CALL polaris.system.expire_snapshots` to prune unreferenced older manifests, followed by an orphan file sweep to reclaim S3 storage."

### Q4: Why use Trino alongside Apache Spark?
> **Answer**:
> "Spark and Trino excel at complementary workloads:
> - **Spark** is our distributed write engine. It handles heavy batch ETL, stateful Structured Streaming from Kafka, and complex `MERGE INTO` joins.
> - **Trino** is our interactive query engine. It uses an in-memory pipelined MPP architecture designed for low-latency ad-hoc SQL, risk dashboards, and regulatory inquiries with sub-second response times.
> Because both engines connect to the same Polaris REST Catalog and Garage S3 bucket, analysts query live trade states via Trino the moment Spark commits a transaction."
