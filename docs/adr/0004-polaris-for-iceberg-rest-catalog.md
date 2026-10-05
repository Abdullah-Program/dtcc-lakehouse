# ADR 0004: Apache Polaris as Open Iceberg REST Catalog

## Context
In Phases 1 through 4, our Apache Iceberg tables were managed using the local `HadoopCatalog` directly pointing to the local filesystem (`/workspace/warehouse/iceberg`). While suitable for single-node PySpark prototyping, `HadoopCatalog` has critical architectural limitations:
1. **Engine Lock-in**: It relies on file-level locking and atomic directory renames, preventing external query engines (Trino, DuckDB, ClickHouse) from safely reading/writing metadata concurrently without Hadoop dependencies.
2. **Catalog Coupling**: Metadata pointers are tied to path locations rather than a centralized, governed catalog service.
3. **No Centralized Governance**: Lacks uniform OAuth2 authentication, role-based access control (RBAC), and multi-tenant table namespace resolution.

To achieve enterprise-grade lakehouse decoupling (Phase 5), we need an open, standard catalog service to govern tables stored in Garage S3 (`s3://dtcc-lakehouse/`).

## Decision
We chose **Apache Polaris** (`apache/polaris:latest`) as our centralized Iceberg REST Catalog.

### Alternatives Considered
- **Hive Metastore (HMS)**: The legacy industry standard. Rejected due to heavy operational baggage (requires dedicated Postgres/MySQL backend, Thrift protocol, heavyweight JVM memory consumption > 1.5 GB, and poor support for modern Iceberg REST specification).
- **Project Nessie**: Git-like catalog for Iceberg. Excellent branch-based metadata capabilities, but Polaris has gained industry consensus as the vendor-neutral Iceberg REST implementation backed by Snowflake and the Apache Software Foundation.
- **AWS Glue Data Catalog / Unity Catalog**: Cloud-specific or vendor-specific proprietary/semi-proprietary solutions that contradict our local, self-contained, open-source stack requirements.

### Key Benefits of Apache Polaris
1. **Iceberg REST Specification**: Implements the official Apache Iceberg REST OpenAPI standard. Any engine (PySpark, Trino, Flink, DuckDB) with an Iceberg REST client can communicate seamlessly without additional plugins.
2. **Lightweight Footprint**: Powered by Quarkus, Polaris boots in seconds and comfortably operates under a strict 512 MB memory boundary (`mem_limit: 512m` in Docker Compose).
3. **Storage Abstraction**: Directly maps the catalog `dtcc_catalog` to Garage S3 (`s3://dtcc-lakehouse/`) using standard S3 endpoint and path-style addressing.
4. **OAuth2 Security & Role Delegation**: Built-in OAuth2 token exchange and RBAC (`service_admin`, `catalog_admin`), preparing the lakehouse for multi-engine access control.

## Consequences
- PySpark configuration in [spark_session.py](file:///d:/programining/dtcc-lakehouse/spark_jobs/common/spark_session.py) uses `org.apache.iceberg.spark.SparkCatalog` with `type=rest`, targeting `http://polaris:8181/api/catalog`.
- Data files and metadata files are persisted to Garage S3 object store (`s3://dtcc-lakehouse/`) via `org.apache.iceberg.aws.s3.S3FileIO`.
- Backward compatibility: The `local` catalog is retained alongside `polaris` in Spark configuration so earlier test suites and Phase 1-4 jobs continue running without disruption.
