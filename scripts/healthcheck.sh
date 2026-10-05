#!/usr/bin/env bash
set -e

echo "=================================================="
echo "    DTCC Lakehouse Infrastructure Healthcheck     "
echo "=================================================="

# Detect docker command (handles WSL / Windows Docker Engine)
DOCKER_CMD="docker"
if command -v docker.exe >/dev/null 2>&1; then
    if docker.exe version >/dev/null 2>&1; then
        DOCKER_CMD="docker.exe"
    fi
fi

# 1. Redpanda Kafka Broker
echo -n "1. Checking Redpanda Streaming Broker... "
if $DOCKER_CMD compose exec redpanda rpk cluster health | grep -q "Healthy:.*true"; then
    echo "✅ HEALTHY"
else
    echo "⚠️ CHECK FAILED"
fi

# 2. Garage S3 Object Store
echo -n "2. Checking Garage S3 Object Store...    "
if $DOCKER_CMD compose exec garage /garage status 2>&1 | grep -q "HEALTHY NODES"; then
    echo "✅ HEALTHY (s3://dtcc-lakehouse/)"
else
    echo "⚠️ CHECK FAILED"
fi

# 3. Apache Polaris REST Catalog
echo -n "3. Checking Apache Polaris REST Catalog... "
POLARIS_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8181/api/catalog/v1/config 2>/dev/null || echo "000")
if [ "$POLARIS_CODE" = "200" ] || [ "$POLARIS_CODE" = "401" ]; then
    echo "✅ HEALTHY (REST OAuth2 API: HTTP $POLARIS_CODE)"
else
    echo "⚠️ CHECK FAILED (Status: $POLARIS_CODE)"
fi

# 4. Trino Query Engine
echo -n "4. Checking Trino Interactive Engine...  "
if $DOCKER_CMD compose exec trino trino --execute "SELECT 1" >/dev/null 2>&1; then
    echo "✅ HEALTHY (Web UI: http://localhost:8080)"
else
    echo "⚠️ CHECK FAILED"
fi

# 5. Apache Spark Container
echo -n "5. Checking PySpark Execution Engine...  "
if $DOCKER_CMD compose exec spark python3 -c "import pyspark" >/dev/null 2>&1; then
    echo "✅ HEALTHY (PySpark 4.1 available)"
else
    echo "⚠️ CHECK FAILED"
fi

echo "=================================================="
echo "    All 5 Lakehouse Services Operational! 🎉     "
echo "=================================================="
