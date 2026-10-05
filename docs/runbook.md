# DTCC Lakehouse Operational Runbook

This runbook provides complete operational procedures for starting, monitoring, testing, maintaining, and troubleshooting the DTCC Open Data Lakehouse platform.

---

## 1. Quick Start & Service Orchestration

### 1.1 Start All Services
From the repository root in WSL:
```bash
# Start all 5 decoupled services in the background
docker compose up -d
```

### 1.2 Run Automated Healthcheck
Verify that all 5 infrastructure layers are operational:
```bash
bash scripts/healthcheck.sh
```

**Expected output:**
```text
==================================================
    DTCC Lakehouse Infrastructure Healthcheck     
==================================================
1. Checking Redpanda Streaming Broker... ✅ HEALTHY
2. Checking Garage S3 Object Store...    ✅ HEALTHY (s3://dtcc-lakehouse/)
3. Checking Apache Polaris REST Catalog... ✅ HEALTHY (REST OAuth2 API: HTTP 401)
4. Checking Trino Interactive Engine...  ✅ HEALTHY (Web UI: http://localhost:8080)
5. Checking PySpark Execution Engine...  ✅ HEALTHY (PySpark 4.1 available)
==================================================
    All 5 Lakehouse Services Operational! 🎉     
==================================================
```

### 1.3 Stop All Services
```bash
docker compose down
```

---

## 2. Ports and Web Interfaces

| Service | Port | Endpoint / Purpose |
| :--- | :--- | :--- |
| **Trino Web UI** | `8080` | `http://localhost:8080` (Cluster overview, live query monitoring) |
| **Polaris REST Catalog** | `8181` | `http://localhost:8181/api/catalog` (OpenAPI REST catalog endpoint) |
| **Garage S3 Storage** | `3900` | `http://localhost:3900` (S3 API endpoint for bucket access) |
| **Redpanda Kafka Broker**| `9092` | `localhost:9092` (Outside client connection) |
| **Redpanda Internal** | `29092`| `redpanda:29092` (Docker internal network) |

---

## 3. Data Pipeline Runbook

### 3.1 Step 1: Batch Silver Table Ingestion
Load raw DTCC cumulative CFTC RATES data into the Silver Iceberg table:
```bash
docker compose exec spark python3 /workspace/spark_jobs/silver/load_silver_iceberg.py --catalog polaris
```

### 3.2 Step 2: Stream Real-Time Events
1. In terminal 1, launch the PySpark Structured Streaming consumer:
   ```bash
   docker compose exec spark python3 /workspace/spark_jobs/streaming/kafka_to_iceberg.py --catalog polaris
   ```
2. In terminal 2, stream live trade events into Redpanda:
   ```bash
   python3 ingestion/kafka_producer.py
   ```

### 3.3 Step 3: Run Trade Corrections Engine (Gold Reconciliation)
Reconcile Silver events into Gold active trade contracts using Iceberg `MERGE INTO`:
```bash
docker compose exec spark python3 /workspace/spark_jobs/gold/apply_corrections.py --catalog polaris
```

### 3.4 Step 4: Verify Zero-Copy Time Travel
Demonstrate regulatory auditing across historical snapshots:
```bash
docker compose exec spark python3 /workspace/spark_jobs/gold/time_travel_demo.py --catalog polaris
```

---

## 4. Trino Interactive Querying

Connect to Trino using the built-in CLI:
```bash
# Show all discovered Iceberg tables
docker compose exec trino trino --execute "SHOW TABLES FROM polaris.dtcc;"

# Run financial risk aggregations
docker compose exec -T trino trino < sql/trino/02_financial_risk_summary.sql

# Run snapshot metadata and time travel audit
docker compose exec -T trino trino < sql/trino/03_time_travel_audit.sql
```

---

## 5. Iceberg Table Maintenance Suite

Run regular maintenance routines to prevent small file degradation and storage bloat:

```bash
# 1. Compact small streaming files (Bin-packing)
docker compose exec spark python3 /workspace/spark_jobs/maintenance/compact_files.py --catalog polaris --table dtcc.silver_rates

# 2. Expire obsolete snapshots (Retain last 2 versions)
docker compose exec spark python3 /workspace/spark_jobs/maintenance/expire_snapshots.py --catalog polaris --table dtcc.gold_active_trades --retain-last 2

# 3. Sweep unreferenced orphan files from Garage S3
docker compose exec -T trino trino < sql/trino/04_maintenance.sql
```

---

## 6. Testing & CI Validation

Run the full automated test suite inside Docker:
```bash
docker compose exec spark python3 -m unittest discover tests
```
