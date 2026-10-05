# Iceberg Table Maintenance Lab Report

## 1. Executive Summary
In high-throughput lakehouse architectures with real-time streaming (Kafka/Redpanda) and ACID upserts (`MERGE INTO`), unmaintained tables quickly degrade in performance and efficiency due to:
1. **The Small Files Problem**: Frequent streaming micro-batches create many tiny Parquet files (< 1 MB), causing high S3 GET overhead and slow query planning.
2. **Snapshot Metadata Bloat**: Every commit adds a snapshot and manifest tree, increasing catalog latency.
3. **Orphan Storage Accumulation**: Expired transactions or aborted writes leave unreferenced Parquet files on S3.

In Phase 6, we designed, implemented, and benchmarked an automated **Table Maintenance Lab** using both **Apache Spark** and **Trino** against our **Apache Polaris REST Catalog** and **Garage S3** object store.

---

## 2. Benchmark Results

### Pillar 1: Small File Compaction (`rewrite_data_files`)
- **Target Table**: `polaris.dtcc.silver_rates` (Streamed from Redpanda)
- **Engine**: PySpark (`spark_jobs/maintenance/compact_files.py`)
- **Algorithm**: `binpack` (Bin-packing small files into consolidated Parquet files without sorting overhead)

| Metric | Before Compaction | After Compaction | Change |
| :--- | :--- | :--- | :--- |
| **Total Data Files** | 6 | 3 | **-50.0% (Cut in half)** |
| **Total Size** | 1,550.75 KB | 1,446.28 KB | -6.7% (Parquet dictionary compression gain) |
| **Average File Size** | 258.46 KB | 482.09 KB | **+86.5% larger / optimal** |
| **Min / Max Size** | 31.43 KB / 1,375.63 KB | 31.43 KB / 1,376.15 KB | Micro-batch files merged |
| **Rewritten Data Files** | — | 5 | Consolidated into 2 files |

---

### Pillar 2: Snapshot Expiration (`expire_snapshots`)
- **Target Table**: `polaris.dtcc.gold_active_trades` (Updated via iterative `MERGE INTO` trade corrections)
- **Engine**: PySpark (`spark_jobs/maintenance/expire_snapshots.py`)
- **Policy**: `retain_last => 2`, `older_than => current_timestamp()`

| Metric | Before Expiration | After Expiration | Purged |
| :--- | :--- | :--- | :--- |
| **Active Snapshots** | 6 | 2 | **4 historical commits removed** |
| **Manifest Files** | 10+ | 3 | **7 manifest files deleted** |
| **Manifest Lists** | 6 | 2 | **4 manifest lists deleted** |
| **Stale Data Files** | 21 | 17 | **4 obsolete data files unlinked** |

---

### Pillar 3: Orphan File Removal (`remove_orphan_files`)
- **Target Table**: `polaris.dtcc.gold_active_trades`
- **Engine**: Trino (`ALTER TABLE polaris.dtcc.gold_active_trades EXECUTE remove_orphan_files(retention_threshold => '7d')`)
- **Storage Swept**: Garage S3 (`s3://dtcc-lakehouse/polaris/dtcc/gold_active_trades/`)

| Metric | Value |
| :--- | :--- |
| **Processed Manifests** | 3 active manifests |
| **Active Referenced Files** | 17 Parquet data files |
| **Total Storage Objects Scanned** | 22 objects |
| **Deleted Unreferenced Files** | 0 (All 22 files protected within 7-day retention safety window) |

---

## 3. Architecture & Operational Best Practices

### The Maintenance Lifecycle Flow
```
┌────────────────────────────────────────────────────────────────────────┐
│                        Lakehouse Ingestion Pipeline                    │
│   (Streaming Kafka Micro-batches + Daily EOD Cumulative MERGE INTO)    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  1. Compaction (Every 2–6 Hours)                       │
│    • CALL system.rewrite_data_files(strategy => 'binpack')             │
│    • Rewrites small files into 128 MB+ files                           │
│    • Zero downtime: Old files remain queryable during rewrite          │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  2. Expire Snapshots (Daily / Weekly)                  │
│    • CALL system.expire_snapshots(retain_last => N)                    │
│    • Unlinks expired snapshot metadata from Polaris REST Catalog       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│               3. Remove Orphan Files (Weekly Maintenance)              │
│    • ALTER TABLE ... EXECUTE remove_orphan_files(retention => '7d')   │
│    • Physically sweeps S3 bucket to clean unreferenced zombie files    │
└────────────────────────────────────────────────────────────────────────┘
```

### Why Order of Operations Matters
1. **Never run orphan file removal before snapshot expiration**: Orphan file cleanup only removes files that are *unreferenced* by manifests. You must first expire old snapshots so their obsolete data files become detached.
2. **Always enforce safety retention thresholds**: In production, never set `retention_threshold => '0s'` on active tables. If a concurrent streaming write is in-flight, setting a 0-second threshold could delete a file while it is being written by Spark. The recommended safety buffer is 3 to 7 days.
