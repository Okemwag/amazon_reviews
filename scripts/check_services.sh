#!/usr/bin/env bash
set -euo pipefail

docker compose ps
docker exec amazon_reviews_namenode hdfs dfs -ls / >/dev/null
docker exec amazon_reviews_spark_master /opt/spark/bin/spark-submit --version >/dev/null
docker exec amazon_reviews_hive_server bash -c "beeline -u jdbc:hive2://localhost:10000 -e 'SHOW DATABASES'" >/dev/null
echo "HDFS, Spark, and Hive checks passed."
