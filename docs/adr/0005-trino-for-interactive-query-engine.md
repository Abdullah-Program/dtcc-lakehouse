# ADR 0005: Trino as Interactive SQL Query Engine

## Context
Up to Phase 5, all analytical and transformation workloads on the DTCC Lakehouse were driven exclusively by Apache Spark (PySpark). While PySpark is the premier engine for distributed ETL, batch preprocessing, and micro-batch streaming, utilizing Spark for ad-hoc analytical queries and dashboard serving presents significant operational drawbacks:
1. **High Query Latency**: Spark requires JVM process initialization, DAG optimization, stage orchestration, and task distribution overhead (often 10–25+ seconds per simple query).
2. **Single-Engine Lock-in**: Relying solely on Spark does not demonstrate true lakehouse decoupling or open table format interoperability.
3. **Analyst Ergonomics**: Financial risk analysts and BI tools require sub-second interactive SQL capabilities without launching resource-intensive Spark executors.

To achieve multi-engine federation over our open lakehouse architecture (Phase 6), we require an interactive Massively Parallel Processing (MPP) query engine that connects directly to the Apache Polaris REST catalog and Garage S3 without duplicating or moving data.

## Decision
We chose **Trino** (`trinodb/trino:latest`) as our interactive query engine.

### Alternatives Considered
- **PrestoDB**: The original fork of Presto. Trino represents the actively maintained, feature-rich evolution with premier Iceberg REST catalog support, native S3 file system, and pushdown optimizations.
- **DuckDB**: Fast in-process analytical engine. While lightweight, DuckDB does not model distributed multi-user query federation, cluster coordinator/worker architecture, or enterprise JDBC/BI concurrency as Trino does.
- **ClickHouse / StarRocks**: Real-time analytical engines. Both support Iceberg, but Trino is the established industry standard for open lakehouse SQL federation alongside Spark and Iceberg.

### Key Benefits of Trino
1. **Direct Iceberg REST Integration**: The `iceberg` connector in Trino natively communicates with Apache Polaris REST Catalog (`http://polaris:8181/api/catalog`) using standard OAuth2 authentication.
2. **True Decoupled Interoperability**: Trino queries the exact same Parquet files written by Spark on Garage S3 (`s3://dtcc-lakehouse/`) using Iceberg snapshot metadata — zero data movement.
3. **Sub-second Interactive SQL**: In-memory pipelined execution model delivers sub-second response times for exploratory aggregations, risk summaries, and trade lookups.
4. **Time Travel SQL Syntax**: Trino natively supports Iceberg snapshot and timestamp queries (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`), matching Spark's time travel capabilities.
5. **Laptop-Friendly Tuning**: Configured with `-Xmx1024M` and container resource limits of `1500M`, Trino operates comfortably alongside Spark, Redpanda, Garage, and Polaris within a 16 GB RAM boundary.

## Consequences
- Added `trino` service to `docker-compose.yml` exposing port `8080` (Trino Web UI and client API).
- Configured `/etc/trino/catalog/polaris.properties` mapping the `polaris` catalog to Polaris REST Catalog and Garage S3 object store.
- Configured `/etc/trino/jvm.config` with G1GC and a 1024 MB heap cap to ensure stability on developer hardware.
