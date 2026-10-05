# Databricks notebook source
# MAGIC %md
# MAGIC # 🏦 DTCC Financial Data Lakehouse on Databricks
# MAGIC ### Multi-Engine Open Lakehouse: PySpark, Delta UniForm & Apache Iceberg
# MAGIC 
# MAGIC This notebook demonstrates ingesting, analyzing, and visualizing DTCC swap derivatives in Databricks.
# MAGIC With **Delta UniForm**, Databricks automatically generates Apache Iceberg metadata, allowing **Snowflake** and **Trino** to query the exact same table concurrently!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Catalog and Schema in Unity Catalog
# MAGIC CREATE CATALOG IF NOT EXISTS dtcc_lakehouse;
# MAGIC USE CATALOG dtcc_lakehouse;
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics;
# MAGIC USE SCHEMA analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold Table with Delta UniForm enabled for Iceberg compatibility
# MAGIC CREATE OR REPLACE TABLE dtcc_gold_active_trades (
# MAGIC     trade_key STRING,
# MAGIC     current_dissemination_id STRING,
# MAGIC     action_type STRING,
# MAGIC     lifecycle_status STRING,
# MAGIC     event_timestamp TIMESTAMP,
# MAGIC     execution_timestamp TIMESTAMP,
# MAGIC     notional_currency STRING,
# MAGIC     notional_amount_leg_1 DOUBLE,
# MAGIC     is_capped_notional_leg_1 BOOLEAN,
# MAGIC     notional_amount_leg_2 DOUBLE,
# MAGIC     is_capped_notional_leg_2 BOOLEAN,
# MAGIC     fixed_rate_leg_1 DOUBLE,
# MAGIC     effective_date STRING,
# MAGIC     expiration_date STRING,
# MAGIC     asset_class STRING,
# MAGIC     _last_file_date STRING,
# MAGIC     _updated_at TIMESTAMP,
# MAGIC     version INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enableIcebergCompatV2' = 'true',
# MAGIC     'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate Representative DTCC Swap Trades
# MAGIC INSERT INTO dtcc_gold_active_trades VALUES
# MAGIC ('TK-IRS-USD-1001', 'DIS-9901', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 09:15:00', TIMESTAMP'2026-10-05 09:14:30', 'USD', 250000000.0, FALSE, 250000000.0, FALSE, 0.0425, '2026-10-06', '2036-10-06', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
# MAGIC ('TK-IRS-USD-1002', 'DIS-9902', 'MODI', 'ACTIVE',     TIMESTAMP'2026-10-05 09:20:00', TIMESTAMP'2026-10-05 09:18:00', 'USD', 500000000.0, FALSE, 500000000.0, FALSE, 0.0410, '2026-10-07', '2031-10-07', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 2),
# MAGIC ('TK-IRS-EUR-2001', 'DIS-9903', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 09:45:00', TIMESTAMP'2026-10-05 09:44:10', 'EUR', 180000000.0, FALSE, 180000000.0, FALSE, 0.0315, '2026-10-08', '2029-10-08', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
# MAGIC ('TK-IRS-GBP-3001', 'DIS-9904', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 10:00:00', TIMESTAMP'2026-10-05 09:58:30', 'GBP', 120000000.0, FALSE, 120000000.0, FALSE, 0.0475, '2026-10-10', '2034-10-10', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
# MAGIC ('TK-IRS-USD-1003', 'DIS-9905', 'TERM', 'TERMINATED', TIMESTAMP'2026-10-05 10:15:00', TIMESTAMP'2026-10-05 10:12:00', 'USD', 75000000.0,  FALSE, 75000000.0,  FALSE, 0.0430, '2026-09-01', '2027-09-01', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 3),
# MAGIC ('TK-CDS-USD-4001', 'DIS-9906', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 10:30:00', TIMESTAMP'2026-10-05 10:28:45', 'USD', 300000000.0, FALSE, 300000000.0, FALSE, 0.0120, '2026-10-06', '2031-10-06', 'Credit:SingleName:Corporate',   '2026-10-05', current_timestamp(), 1),
# MAGIC ('TK-IRS-CAD-5001', 'DIS-9907', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 11:00:00', TIMESTAMP'2026-10-05 10:55:00', 'CAD', 95000000.0,  FALSE, 95000000.0,  FALSE, 0.0380, '2026-10-12', '2028-10-12', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1),
# MAGIC ('TK-IRS-USD-1004', 'DIS-9908', 'NEW',  'ACTIVE',     TIMESTAMP'2026-10-05 11:20:00', TIMESTAMP'2026-10-05 11:18:20', 'USD', 650000000.0, FALSE, 650000000.0, FALSE, 0.0405, '2026-10-15', '2046-10-15', 'InterestRate:IRSwap:FixedFloat', '2026-10-05', current_timestamp(), 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Executive Analytics & Built-In Visualizations
# MAGIC Run `display()` to invoke Databricks interactive charts and heatmaps.

# COMMAND ----------

summary_df = spark.sql("""
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
    ORDER BY notional_billions DESC
""")

display(summary_df)
