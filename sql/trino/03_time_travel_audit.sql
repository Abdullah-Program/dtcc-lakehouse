-- Iceberg Snapshot Metadata Inspection and Time Travel in Trino

-- 1. Inspect all snapshot commits made by Spark and Streaming jobs
SELECT
    snapshot_id,
    parent_id,
    operation,
    committed_at,
    summary['total-records'] AS total_records,
    summary['added-records'] AS added_records
FROM polaris.dtcc."gold_active_trades$snapshots"
ORDER BY committed_at DESC;

-- 2. Inspect Iceberg manifest files
SELECT
    path,
    length,
    partition_spec_id,
    added_snapshot_id,
    added_data_files_count
FROM polaris.dtcc."gold_active_trades$manifests"
LIMIT 10;
