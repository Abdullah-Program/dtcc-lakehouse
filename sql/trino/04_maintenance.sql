-- Iceberg Table Maintenance Lab via Trino
-- 1. Sweep and remove orphan files from Garage S3
ALTER TABLE polaris.dtcc.gold_active_trades EXECUTE remove_orphan_files(retention_threshold => '7d');

-- 2. Native Table Optimization (Compaction) in Trino
ALTER TABLE polaris.dtcc.silver_rates EXECUTE optimize;
