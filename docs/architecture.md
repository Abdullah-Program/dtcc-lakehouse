# DTCC Open Data Lakehouse Architecture Deep Dive

## 1. System Topology & Data Flow

The DTCC Open Data Lakehouse platform ingests real-time and end-of-day swap trade messages from the Depository Trust & Clearing Corporation (DTCC), processes and normalizes events through a Medallion Architecture, and exposes curated analytical datasets through decoupled, open-standard interfaces.

```
                           ┌──────────────────────────────────────────────┐
                           │          DTCC Public Swap Data Feeds         │
                           │     • Live Ticker JSON  • EOD Daily ZIP      │
                           └──────────────────────┬───────────────────────┘
                                                  │
                 ┌────────────────────────────────┴────────────────────────────────┐
                 │                                                                 │
                 ▼ (Real-Time Ingestion)                                           ▼ (Batch End-of-Day)
      ┌─────────────────────────┐                                       ┌─────────────────────────┐
      │   Redpanda Broker       │                                       │   Bronze Landing Layer  │
      │   (Kafka API Topic)     │                                       │   (Snappy Parquet)      │
      │   dtcc.rates.raw        │                                       │   warehouse/bronze/     │
      └────────────┬────────────┘                                       └────────────┬────────────┘
                   │                                                                 │
                   ▼ (Micro-batches: 1s trigger)                                     ▼ (PySpark Transformation)
      ┌───────────────────────────────────────────────────────────────────────────────────────────┐
      │                               Apache Spark (PySpark 4.1)                                  │
      │   • Clean Headers (snake_case)  • Cast Timestamps  • Derive trade_key  • Sanitize Notionals   │
      └─────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                                    │
                                                    ▼
      ┌───────────────────────────────────────────────────────────────────────────────────────────┐
      │                            Apache Polaris (Iceberg REST Catalog)                          │
      │   • Centralized Metadata Governance  • OpenAPI REST Standard  • OAuth2 Token Security     │
      └─────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                                    │
                       ┌────────────────────────────┴────────────────────────────┐
                       ▼                                                         ▼
      ┌───────────────────────────────────┐                     ┌───────────────────────────────────┐
      │           Apache Spark            │                     │               Trino               │
      │          (Write Engine)           │                     │          (Query Engine)           │
      ├───────────────────────────────────┤                     ├───────────────────────────────────┤
      │ • PySpark Structured Streaming    │                     │ • Massively Parallel Processing   │
      │ • ACID MERGE INTO Gold Engine     │                     │ • Sub-second Financial Queries    │
      │ • File Compaction (Bin-pack)      │                     │ • Web UI Dashboard (Port 8080)    │
      │ • Expire Snapshots Pruning        │                     │ • Orphan File Sweep & GC          │
      └─────────────────┬─────────────────┘                     └─────────────────┬─────────────────┘
                        │                                                         │
                        └───────────────────────────┬─────────────────────────────┘
                                                    ▼
      ┌───────────────────────────────────────────────────────────────────────────────────────────┐
      │                               Garage S3 Object Storage                                    │
      │                                (s3://dtcc-lakehouse/)                                     │
      ├───────────────────────────────────────────────────────────────────────────────────────────┤
      │  polaris/dtcc/silver_rates/       (117 columns, partitioned by file_date, append-only)    │
      │  polaris/dtcc/gold_active_trades/ (17 columns, reconciled state table, versioned snapshots) │
      └───────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Medallion Architecture Specifications

### 2.1 Bronze Layer (Raw Ingestion)
- **Format**: Apache Parquet with Snappy compression.
- **Location**: `warehouse/bronze/rates/file_date=YYYY-MM-DD/`
- **Schema**: Exact 110 raw DTCC columns preserved as strings with ingestion metadata:
  - `_ingested_at` (`TimestampType`): UTC timestamp when the record landed.
  - `_source_file` (`StringType`): Originating archive filename.

### 2.2 Silver Layer (Cleaned & Partitioned Events)
- **Table**: `polaris.dtcc.silver_rates`
- **Format**: Apache Iceberg (Format Version 2).
- **Partitioning**: Identity-partitioned by `file_date` (`YYYY-MM-DD`).
- **Core Transformations**:
  - Headers sanitized into standardized `snake_case` (e.g. `Dissemination Identifier` -> `dissemination_identifier`).
  - Dates and timestamps parsed into ISO `TimestampType`.
  - Notional amounts stripped of commas, caps, and converted to `DoubleType`.
  - Deterministic `trade_key` derivation:
    $$\text{trade\_key} = \begin{cases} \text{dissemination\_identifier} & \text{if Action} = \text{'NEWT'} \\ \text{original\_dissemination\_identifier} & \text{otherwise} \end{cases}$$

### 2.3 Gold Layer (Curated Trade State Table)
- **Table**: `polaris.dtcc.gold_active_trades`
- **Format**: Apache Iceberg (Format Version 2).
- **Primary Key**: `trade_key` (enforced via `MERGE INTO` unique constraint).
- **Regulatory Lifecycle Mapping**:
  $$\text{lifecycle\_status} = \begin{cases} 
  \text{'ACTIVE'} & \text{if Action} \in \{\text{NEWT, MODI, CORR, REVI}\} \\
  \text{'TERMINATED'} & \text{if Action} = \text{'TERM'} \\
  \text{'CANCELLED'} & \text{if Action} = \text{'EROR'} 
  \end{cases}$$
- **Schema (17 Core Attributes)**:
  `trade_key`, `current_dissemination_id`, `action_type`, `lifecycle_status`, `event_timestamp`, `execution_timestamp`, `notional_amount_leg_1`, `is_capped_notional_leg_1`, `notional_amount_leg_2`, `is_capped_notional_leg_2`, `fixed_rate_leg_1`, `effective_date`, `expiration_date`, `asset_class`, `_last_file_date`, `_updated_at`, `version`.

---

## 3. ACID Transactions & Concurrency

### 3.1 Snapshot Isolation
Apache Iceberg utilizes an **immutable metadata tree**:
- Every write (append, delete, overwrite) generates a new snapshot containing an immutable manifest list.
- Readers always query an atomic snapshot commit, completely isolated from concurrent streaming writes or maintenance compaction.

### 3.2 Optimistic Concurrency Control (OCC)
When PySpark executes `MERGE INTO`, it evaluates the target table's latest snapshot. If concurrent writes conflict on the same data files, Iceberg automatically retries the commit against the newly created snapshot without locking the entire table.

---

## 4. Multi-Engine Query Federation

| Characteristic | Apache Spark (PySpark 4.1) | Trino 483 |
| :--- | :--- | :--- |
| **Primary Workload** | Heavy ETL, Streaming Ingest, Compaction | Interactive SQL, BI Dashboards, Risk Analysis |
| **Execution Architecture** | Distributed Stage DAG / Executor JVMs | In-Memory Pipelined MPP Execution |
| **Query Latency** | 10–30+ seconds (DAG startup overhead) | Sub-second (50–500 ms) |
| **Catalog Protocol** | Iceberg REST Client via `S3FileIO` | Native Iceberg REST Connector via `fs.s3` |
| **Memory Boundary** | Capped at 2500M in Docker Compose | Capped at 1500M (`-Xmx1024M` JVM Heap) |

---

## 5. Automated Table Maintenance

Streaming pipelines and iterative upserts naturally create small file sprawl and snapshot bloat. The platform implements automated maintenance routines:

1. **File Compaction (`rewrite_data_files`)**:
   - Bin-packs small micro-batch files into balanced Parquet files.
   - Result: 50% reduction in file handles (6 -> 3 files) and 86.5% increase in average file size.
2. **Snapshot Expiration (`expire_snapshots`)**:
   - Enforces a 2-version audit retention policy (`retain_last => 2`).
   - Purges stale snapshots, manifests, and unreferenced Parquet files.
3. **Orphan File Sweep (`remove_orphan_files`)**:
   - Audits Garage S3 storage via Trino's native S3 engine against active manifest trees with a 7-day safety retention buffer.
