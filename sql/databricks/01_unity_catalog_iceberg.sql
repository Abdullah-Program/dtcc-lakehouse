-- =====================================================================
-- Databricks Unity Catalog - Open Lakehouse Integration & UniForm
-- =====================================================================
-- Demonstrates managing DTCC Swap data in Databricks Unity Catalog while
-- maintaining 100% interoperability with Snowflake and Trino via Delta UniForm (Iceberg).

-- 1. Select Catalog & Schema in Unity Catalog
USE CATALOG workspace;
USE SCHEMA default;

-- 2. Create Gold Analytics Table with Delta UniForm (Universal Format)
-- Enabling 'delta.universalFormat.enabledFormats' = 'iceberg' allows
-- Databricks to automatically generate Iceberg metadata so external engines
-- (Trino, Snowflake, Spark) can query it with zero data copying.
CREATE OR REPLACE TABLE dtcc_gold_active_trades (
    trade_key STRING,
    current_dissemination_id STRING,
    action_type STRING,
    lifecycle_status STRING,
    event_timestamp TIMESTAMP,
    execution_timestamp TIMESTAMP,
    notional_currency STRING,
    notional_amount_leg_1 DOUBLE,
    is_capped_notional_leg_1 BOOLEAN,
    notional_amount_leg_2 DOUBLE,
    is_capped_notional_leg_2 BOOLEAN,
    fixed_rate_leg_1 DOUBLE,
    effective_date STRING,
    expiration_date STRING,
    asset_class STRING,
    _last_file_date STRING,
    _updated_at TIMESTAMP,
    version INT
)
USING DELTA
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- 3. Populate Sample Real-World DTCC Derivative Trades
INSERT INTO dtcc_gold_active_trades VALUES
('TK-IRS-USD-1001', 'DIS-9901', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 09:15:00', TIMESTAMP'2026-10-05 09:14:30', 'USD', 250000000.0, FALSE, 250000000.0, FALSE, 0.0425, '2026-10-06', '2036-10-06', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
('TK-IRS-USD-1002', 'DIS-9902', 'MODI', 'ACTIVE',     TIMESTAMP'2026-10-05 09:20:00', TIMESTAMP'2026-10-05 09:18:00', 'USD', 500000000.0, FALSE, 500000000.0, FALSE, 0.0410, '2026-10-07', '2031-10-07', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 2),
('TK-IRS-EUR-2001', 'DIS-9903', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 09:45:00', TIMESTAMP'2026-10-05 09:44:10', 'EUR', 180000000.0, FALSE, 180000000.0, FALSE, 0.0315, '2026-10-08', '2029-10-08', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
('TK-IRS-GBP-3001', 'DIS-9904', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 10:00:00', TIMESTAMP'2026-10-05 09:58:30', 'GBP', 120000000.0, FALSE, 120000000.0, FALSE, 0.0475, '2026-10-10', '2034-10-10', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
('TK-IRS-USD-1003', 'DIS-9905', 'TERM', 'TERMINATED', TIMESTAMP'2026-10-05 10:15:00', TIMESTAMP'2026-10-05 10:12:00', 'USD', 75000000.0,  FALSE, 75000000.0,  FALSE, 0.0430, '2026-09-01', '2027-09-01', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 3),
('TK-CDS-USD-4001', 'DIS-9906', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 10:30:00', TIMESTAMP'2026-10-05 10:28:45', 'USD', 300000000.0, FALSE, 300000000.0, FALSE, 0.0120, '2026-10-06', '2031-10-06', 'Credit:SingleName:Corporate',   '2026-10-05', current_timestamp(), 1),
('TK-IRS-CAD-5001', 'DIS-9907', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 11:00:00', TIMESTAMP'2026-10-05 10:55:00', 'CAD', 95000000.0,  FALSE, 95000000.0,  FALSE, 0.0380, '2026-10-12', '2028-10-12', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
('TK-IRS-USD-1004', 'DIS-9908', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 11:20:00', TIMESTAMP'2026-10-05 11:18:20', 'USD', 650000000.0, FALSE, 650000000.0, FALSE, 0.0405, '2026-10-15', '2046-10-15', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1);

-- 4. Interactive Financial Analytics Query
SELECT 
    notional_currency,
    asset_class,
    count(*) AS total_trades,
    round(sum(notional_amount_leg_1) / 1e6, 2) AS notional_millions,
    round(sum(notional_amount_leg_1) / 1e9, 3) AS notional_billions,
    round(avg(fixed_rate_leg_1) * 100, 2) AS avg_fixed_rate_pct
FROM dtcc_gold_active_trades
WHERE lifecycle_status = 'ACTIVE'
GROUP BY notional_currency, asset_class
ORDER BY notional_billions DESC;
