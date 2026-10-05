-- Verify Polaris REST Catalog and DTCC Tables in Trino
SHOW CATALOGS;

SHOW SCHEMAS FROM polaris;

SHOW TABLES FROM polaris.dtcc;

-- Inspect Table Column Schema
DESCRIBE polaris.dtcc.silver_rates;
DESCRIBE polaris.dtcc.gold_active_trades;
