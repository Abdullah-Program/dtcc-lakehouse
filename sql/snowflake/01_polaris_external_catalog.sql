-- Snowflake Integration with Apache Polaris REST Catalog and S3 Storage
-- Demonstrates querying the open DTCC Lakehouse Iceberg tables from Snowflake without data ingestion or vendor lock-in.

-- 1. Create External Volume pointing to the S3 bucket
CREATE OR REPLACE EXTERNAL VOLUME dtcc_lakehouse_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'garage-s3-dtcc'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://dtcc-lakehouse/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-iceberg-access-role'
    )
  );

-- 2. Create Catalog Integration for Apache Polaris REST Catalog
CREATE OR REPLACE CATALOG INTEGRATION polaris_rest_catalog
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'dtcc'
  REST_CONFIG = (
    CATALOG_URI = 'http://polaris:8181/api/catalog',
    WAREHOUSE = 'dtcc_catalog'
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH2,
    OAUTH_CLIENT_ID = 'root',
    OAUTH_CLIENT_SECRET = 's3cr3t',
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE;

-- 3. Create Iceberg Tables referencing Polaris managed metadata
CREATE OR REPLACE ICEBERG TABLE dtcc_gold_active_trades
  EXTERNAL_VOLUME = 'dtcc_lakehouse_volume'
  CATALOG = 'polaris_rest_catalog'
  CATALOG_TABLE_NAME = 'gold_active_trades';

CREATE OR REPLACE ICEBERG TABLE dtcc_silver_rates
  EXTERNAL_VOLUME = 'dtcc_lakehouse_volume'
  CATALOG = 'polaris_rest_catalog'
  CATALOG_TABLE_NAME = 'silver_rates';

-- 4. Interactive Financial Queries in Snowflake
-- Query active trades directly from S3 Parquet files via Polaris REST metadata:
SELECT 
    asset_class,
    lifecycle_status,
    count(*) AS total_trades,
    round(sum(notional_amount_leg_1) / 1e9, 2) AS total_notional_billions
FROM dtcc_gold_active_trades
WHERE lifecycle_status = 'ACTIVE'
GROUP BY asset_class, lifecycle_status;
