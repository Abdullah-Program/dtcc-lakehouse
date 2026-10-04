# DTCC Open Data Lakehouse: Trade Corrections Engine

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.3-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.11.0-blue?logo=apache&logoColor=white)](https://iceberg.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.14%20%7C%203.10-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Desktop%20%2B%20WSL2-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An open-source financial lakehouse built from scratch to process, clean, audit, and reconcile live public derivatives trade records from the **Depository Trust & Clearing Corporation (DTCC)**. 

Designed as a production-grade portfolio project modeling modern **AWS Open Data Platform** patterns (PySpark, Apache Iceberg, Apache Polaris REST catalog, Garage S3, Redpanda/Kafka, Trino, and Snowflake), running 100% free and resource-bounded on a developer workstation.

---

## Table of Contents
1. [What is DTCC and Why Does This Data Exist?](#1-what-is-dtcc-and-why-does-this-data-exist)
2. [The Core Problem: Why is this Lakehouse Needed?](#2-the-core-problem-why-is-this-lakehouse-needed)
3. [Architecture: Medallion Lakehouse Design](#3-architecture-medallion-lakehouse-design)
4. [Trade Lifecycle & Data Modeling](#4-trade-lifecycle--data-modeling)
5. [Technology Stack & Design Choices](#5-technology-stack--design-choices)
6. [Repository Structure](#6-repository-structure)
7. [Implementation Roadmap & Current Progress](#7-implementation-roadmap--current-progress)
8. [Local Quickstart & Verification](#8-local-quickstart--verification)

---

## 1. What is DTCC and Why Does This Data Exist?

The **Depository Trust & Clearing Corporation (DTCC)** is the premier post-trade market infrastructure for the global financial services industry. In 2023 alone, DTCC subsidiaries processed securities transactions valued at nearly **$3 quadrillion**.

```
[ Financial Market Participants ]
(Goldman Sachs, JPMorgan, Citadel, Hedge Funds, Pension Funds)
                    │
                    ▼  Executes OTC Derivatives (Interest Rate Swaps, Credit Swaps)
         [ Dodd-Frank Act Title VII ]
                    │
                    ▼  Mandatory Real-Time Public Dissemination (PPD)
[ DTCC Real-Time Swap Data Repository (SDR) ]
                    │
                    ▼  Public S3 Bucket (JSON Manifests + Daily CSV/Zips)
      [ DTCC Open Data Lakehouse ]
```

### Regulatory Origins: Dodd-Frank Title VII
During the 2008 global financial crisis, bilateral **Over-The-Counter (OTC) derivatives** (such as credit default swaps and interest rate swaps) were opaque. Counterparties could not assess market-wide exposure or fair market prices, contributing to systemic collapse.

In response, the U.S. Congress enacted the **Dodd-Frank Wall Street Reform and Consumer Protection Act (2010)**. Under **Title VII**, the **Commodity Futures Trading Commission (CFTC)** mandated that all market participants report OTC swap transactions to registered **Swap Data Repositories (SDRs)**, such as DTCC.

DTCC publicly disseminates these transactions in real time via its **Public Price Dissemination (PPD)** service:
- **Ticker / Slice Feeds**: Micro-batch updates disseminated every few seconds as JSON manifests.
- **Cumulative End-Of-Day (EOD) Feeds**: Complete daily snapshot archives containing hundreds of thousands of trade messages across 110+ regulatory attributes.

---

## 2. The Core Problem: Why is this Lakehouse Needed?

Processing financial derivatives is fundamentally different from analyzing static transactional data (like e-commerce orders or website clickstreams).

### 1. Complex Trade Lifecycles
A financial contract (e.g., a 10-year SOFR interest rate swap) can exist for years or decades. During its lifespan, it generates a stream of distinct regulatory messages:
- **`NEWT` (New Trade)**: Creation of the initial contract.
- **`MODI` (Modification)**: Mutual adjustment of terms (notional, maturity date, floating leg rate).
- **`CORR` (Correction)**: Remediation of clerical reporting errors (e.g., misspelled currency code, wrong notional amount).
- **`TERM` (Termination)**: Early unwind or contract settlement.
- **`EROR` (Error/Cancellation)**: Accidental submission that should be completely stricken from market records.
- **`REVI` (Revision)**: Post-dissemination regulatory adjustment.

### 2. The Out-of-Order & Cross-File Challenge
Empirical analysis of DTCC daily files reveals that:
- **~20% of rows are mutations** (`MODI`, `CORR`, `TERM`, `EROR`, `REVI`), not new trades.
- **~32% of modifications refer to root trades from previous days or weeks** that are not present in that day's file.
- Individual contracts can experience up to **21 consecutive modifications and corrections**.

### 3. The Dual Analytical Requirement
Downstream consumers have two conflicting data requirements:
1. **Regulators & Compliance Auditors**: Demand an immutable, full historical audit trail. They must be able to ask: *"What did the market record for Trade X look like on 2026-10-02 at 14:30:00 UTC?"* (Requires full event history and **Time Travel**).
2. **Risk Engines & Portfolio Managers**: Demand the single current, deduplicated truth. They must be able to ask: *"What is the active outstanding notional and fixed rate for Trade X right now?"* (Requires stateful **Upserts** / `MERGE INTO`).

### 4. Why Traditional Formats Fail & Why Apache Iceberg Wins
| Requirement | Flat Parquet / CSV | Traditional Data Warehouse | Apache Iceberg Lakehouse |
| :--- | :--- | :--- | :--- |
| **Row-level Upserts** | Requires full file/partition rewrite | Supported, but proprietary lock-in | Native ACID `MERGE INTO` via copy-on-write or merge-on-read |
| **Historical Auditing** | Manual date snapshots, high storage cost | Expensive table backups | Native **Time Travel** via zero-copy metadata snapshots |
| **Streaming Ingestion** | Produces millions of unmanageable small files | High ingest compute bills | Controlled commit log with background compaction |
| **Multi-Engine Access** | Prone to read-during-write corruption | Siloed; only accessible via that vendor | Open format readable by Spark, Trino, Snowflake, Athena |

---

## 3. Architecture: Medallion Lakehouse Design

The project implements a **Medallion Architecture** (Bronze $\to$ Silver $\to$ Gold) utilizing Apache Iceberg tables governed by a central catalog.

```
                      ┌──────────────────────────────────────┐
                      │        DTCC Public Data S3           │
                      │  https://<bucket>.s3.amazonaws.com   │
                      └──────────────────┬───────────────────┘
                                         │
                   HTTP Ingest / Cache   │   Polling Ticker / Slice
                                         ▼
                      ┌──────────────────────────────────────┐
                      │          Local / S3 Storage          │
                      │   (samples/ or Garage S3 Bucket)     │
                      └──────────────────┬───────────────────┘
                                         │
 ┌───────────────────────────────────────┴───────────────────────────────────────┐
 │                                                                               │
 │  BRONZE LAYER (Raw Ingestion)                                                 │
 │  - Raw fidelity (inferSchema=false, 100% strings preserved)                   │
 │  - Lakehouse audit columns (_ingested_at, _source_file, file_date)            │
 │  - Partitioned Snappy Parquet (warehouse/bronze/rates/file_date=YYYY-MM-DD/)  │
 │                                                                               │
 └───────────────────────────────────────┬───────────────────────────────────────┘
                                         │
                                         ▼ [spark_jobs/silver/clean_trades.py]
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │  SILVER LAYER (Standardized & Typed Lakehouse)                                │
 │  - Header sanitization: 'Dissemination Identifier' -> 'dissemination_id'     │
 │  - Trade key linkage: trade_key = coalesce(original_id, own_id)               │
 │  - Strict type casting: ISO-8601 timestamps, numeric doubles                  │
 │  - CFTC regulatory cap indicator handling ('470,000,000+' -> 4.7e8 + flag)    │
 │  - Apache Iceberg Table: local.dtcc.silver_rates (Partitioned by file_date)   │
 │                                                                               │
 └───────────────────────────────────────┬───────────────────────────────────────┘
                                         │
                                         ▼ [spark_jobs/gold/apply_corrections.py]
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │  GOLD LAYER (Trade Corrections Engine - Serving)                              │
 │  - Stateful upserts via Iceberg MERGE INTO on trade_key                       │
 │  - Tracks trade lifecycle: ACTIVE, MODIFIED, CORRECTED, TERMINATED, CANCELLED │
 │  - Snapshot isolation: full time-travel query support for compliance audit    │
 │  - Apache Iceberg Table: local.dtcc.gold_active_trades                        │
 │                                                                               │
 └───────────────────────────────────────┬───────────────────────────────────────┘
                                         │
              ┌──────────────────────────┴──────────────────────────┐
              ▼                                                     ▼
┌───────────────────────────┐                         ┌───────────────────────────┐
│     Trino Query Engine    │                         │    Snowflake / BI Tools   │
│  (Interactive Analytics)  │                         │    (External Iceberg)     │
└───────────────────────────┘                         └───────────────────────────┘
```

---

## 4. Trade Lifecycle & Data Modeling

### The 6 Regulatory Action Types
| Action Type | Meaning | Operational Handling in Lakehouse |
| :---: | :--- | :--- |
| **`NEWT`** | New Transaction | Inserted as a new trade lifecycle root (`trade_key = dissemination_identifier`). |
| **`MODI`** | Modification | Updates contract terms for an existing `trade_key`. |
| **`CORR`** | Correction | Overwrites previous erroneous values for an existing `trade_key`. |
| **`TERM`** | Termination | Marks the contract state as closed/settled. |
| **`EROR`** | Reporting Error | Invalidates/cancels the trade record so it is excluded from active analytics. |
| **`REVI`** | Revision | Applies post-trade regulatory amendments. |

### The `trade_key` Derivation Rule
To trace an entire trade chain across days, the engine derives a surrogate `trade_key`:
$$\text{trade\_key} = \begin{cases} 
\text{original\_dissemination\_identifier} & \text{if populated and non-empty} \\
\text{dissemination\_identifier} & \text{otherwise (root NEWT trade)}
\end{cases}$$

### Regulatory Notional Caps
Under CFTC public disclosure regulations, massive institutional trades are capped to preserve market liquidity anonymity (e.g., `470,000,000+`). The cleaning pipeline extracts the clean float while setting a boolean flag:
- `notional_amount_leg_1`: `470000000.0`
- `is_capped_notional_leg_1`: `true`

---

## 5. Technology Stack & Design Choices

| Component | Selected Technology | Why Chosen Over Alternatives |
| :--- | :--- | :--- |
| **Processing Engine** | **Apache Spark 4.1.3 (PySpark)** | Industry standard for distributed processing. Deployed in Docker with strict memory caps (1500M driver) to guarantee reliable execution on modest laptop hardware. |
| **Table Format** | **Apache Iceberg 1.11.0** | Chosen over Delta Lake due to open standard vendor neutrality, superior multi-engine support (Trino, Snowflake, Athena), and native catalog flexibility. |
| **Catalog** | **Apache Polaris / Hadoop Catalog** | Open-source, vendor-neutral REST catalog supporting multi-engine data governance without proprietary lock-in. |
| **Object Storage** | **Garage S3 (Local Filesystem)** | Lightweight, modern Rust-based distributed S3 service. Selected because MinIO changed to AGPL and has been archived in modern enterprise open stacks. |
| **Streaming** | **Redpanda** | 100% Kafka-API compatible, written in C++ with zero JVM overhead and minimal memory footprint, ideal for local streaming development. |
| **Query Engine** | **Trino** | Distributed SQL query engine providing fast, interactive queries across Iceberg tables. |

---

## 6. Repository Structure

```text
dtcc-lakehouse/
├── .github/                      # CI/CD workflows and issue templates
│   ├── workflows/ci.yml          # GitHub Actions linting and automated test runs
│   └── ISSUE_TEMPLATE/ticket.md  # Standardized issue template
├── benchmarks/                   # Query performance benchmarks and metrics
├── docs/                         # Technical documentation and ADRs
│   ├── adr/                      # Architecture Decision Records
│   ├── data-notes.md             # Empirical findings on DTCC dataset columns & types
│   ├── project-context.md        # AI assistant rules, constraints, and environment facts
│   └── runbook.md                # Step-by-step operational runbook
├── infra/                        # Infrastructure as code & container configurations
│   ├── garage/                   # S3 object storage configuration
│   ├── polaris/                  # Apache Polaris REST catalog configuration
│   └── trino/                    # Trino distributed SQL coordinator/worker configs
├── ingestion/                    # DTCC ingestion, exploratory scripts, and producers
│   ├── inspect_dtcc.py           # Resolves DTCC bucket dynamically and downloads samples
│   ├── explore_actions.py        # Explores action types and original ID distributions
│   ├── explore_chains.py         # Traces trade lineage and parent-child ID ordering
│   └── kafka_producer.py         # Streams DTCC messages into Redpanda
├── monitoring/                   # Observability stack (Prometheus & Grafana)
├── orchestration/                # Pipeline orchestrator configurations
├── samples/                      # (Gitignored) Cached sample JSON manifests and raw CSVs
├── spark_jobs/                   # PySpark Lakehouse pipeline jobs
│   ├── common/                   # Shared utilities (SparkSession factory, schemas)
│   │   └── spark_session.py      # Factory configuring Iceberg runtime & memory caps
│   ├── bronze/                   # Raw Parquet ingestion jobs
│   │   ├── read_raw_rates.py     # Schema validation and raw CSV inspection
│   │   └── load_cumulative.py    # Bronze ingestion with Lakehouse audit metadata
│   ├── silver/                   # Cleaning, standardizing, and typing jobs
│   │   └── clean_trades.py       # Snake_case, trade_key derivation, type casting
│   ├── gold/                     # Serving layer & trade corrections engine
│   │   └── apply_corrections.py  # Iceberg MERGE INTO state aggregation
│   ├── maintenance/              # Iceberg table optimization and maintenance
│   │   ├── compact_files.py      # Small file compaction
│   │   ├── expire_snapshots.py   # Snapshot pruning
│   │   └── remove_orphans.py     # Orphan data file deletion
│   ├── streaming/                # Real-time structured streaming jobs
│   ├── hello_spark.py            # Container environment verification script
│   └── iceberg_smoke_test.py     # Iceberg 1.11.0 ACID smoke test
├── tests/                        # Automated unit and integration tests
│   └── test_silver_clean.py      # Unit tests for Silver layer cleaning logic
├── warehouse/                    # (Gitignored) Local lakehouse storage (Bronze, Silver, Iceberg)
├── docker-compose.yml            # Docker services definition (Spark, Iceberg, memory limits)
├── progress.md                   # Chronological project tracker and environment specs
└── README.md                     # Root project documentation and architectural guide
```

---

## 7. Implementation Roadmap & Current Progress

- [x] **Phase 0: Environment Setup & DTCC Exploration**
  - Git repository structure, Docker PySpark container, and DTCC API exploration scripts.
  - Empirical analysis of action types, IDs, and trade lifecycles on live data.
- [x] **Phase 1: PySpark Foundations & Medallion Pipeline**
  - Raw CSV ingestion and type validation without data loss ([read_raw_rates.py](file:///d:/programining/dtcc-lakehouse/spark_jobs/bronze/read_raw_rates.py)).
  - Partitioned Bronze Parquet creation with audit metadata ([load_cumulative.py](file:///d:/programining/dtcc-lakehouse/spark_jobs/bronze/load_cumulative.py)).
  - Silver layer standardizing, `trade_key` linking, and typing ([clean_trades.py](file:///d:/programining/dtcc-lakehouse/spark_jobs/silver/clean_trades.py)).
- [ ] **Phase 2: Apache Iceberg Integration** *(In Progress)*
  - [x] **Step 2.1**: Configure Iceberg 1.11.0 runtime, extensions, and local catalog. Verified via [iceberg_smoke_test.py](file:///d:/programining/dtcc-lakehouse/spark_jobs/iceberg_smoke_test.py).
  - [ ] **Step 2.2**: Load cleaned Silver data into Iceberg table `local.dtcc.silver_rates`.
- [ ] **Phase 3: The Trade Corrections Engine**
  - Implement Gold layer `MERGE INTO` logic to reconcile `MODI`, `CORR`, `TERM`, and `EROR` states into active trades.
  - Implement Time Travel compliance audit queries.
- [ ] **Phase 4: Streaming Ingestion**
  - Deploy Redpanda and stream live DTCC ticker messages into Iceberg tables via Spark Structured Streaming.
- [ ] **Phase 5: Storage & Catalog Decoupling**
  - Deploy Garage S3 and Apache Polaris REST catalog.
- [ ] **Phase 6: Trino & The Iceberg Maintenance Lab**
  - Query Iceberg tables via Trino; benchmark compaction, snapshot expiration, and orphan cleanup.
- [ ] **Phase 7: CI/CD & Production Observability**
  - Automated GitHub Actions test suites and Prometheus/Grafana pipeline monitoring.
- [ ] **Phase 8: Cloud Expansion & Portfolio Write-Up**
  - Snowflake External Iceberg table integration and technical project presentation.

---

## 8. Local Quickstart & Verification

All operations run inside a memory-constrained Docker container to isolate dependencies and prevent host memory exhaustion.

### 1. Prerequisites
- Docker Desktop with WSL2 backend enabled.
- Python 3.9+ on host/WSL for lightweight exploration scripts.

### 2. Start the PySpark Container
```bash
docker compose up -d
docker compose ps
```

### 3. Verify PySpark & Iceberg Runtime
Run the smoke tests inside the running `dtcc_spark` container:
```bash
# Verify PySpark DataFrame operations
docker compose exec spark python3 /workspace/spark_jobs/hello_spark.py

# Verify Apache Iceberg 1.11.0 catalog, table creation, ACID inserts, and snapshots
docker compose exec spark python3 /workspace/spark_jobs/iceberg_smoke_test.py
```

### 4. Run the Data Pipeline (Bronze $\to$ Silver)
```bash
# 1. Read and validate raw DTCC CSV
docker compose exec spark python3 /workspace/spark_jobs/bronze/read_raw_rates.py

# 2. Ingest raw data into partitioned Bronze Parquet
docker compose exec spark python3 /workspace/spark_jobs/bronze/load_cumulative.py

# 3. Clean, derive trade_key, cast types, and output Silver Parquet
docker compose exec spark python3 /workspace/spark_jobs/silver/clean_trades.py

# 4. Run unit tests
docker compose exec spark python3 -m unittest discover -s /workspace/tests -p "test_*.py"
```
