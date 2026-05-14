#!/usr/bin/env bash
set -euo pipefail

docker exec amazon_reviews_hive_server bash -c 'beeline -u jdbc:hive2://localhost:10000 -f /workspace/hive/queries/01_exploratory_analysis.sql'
docker exec amazon_reviews_hive_server bash -c 'beeline -u jdbc:hive2://localhost:10000 -f /workspace/hive/queries/02_business_insights.sql'
