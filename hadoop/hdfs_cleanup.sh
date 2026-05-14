#!/usr/bin/env bash
set -euo pipefail

NAMENODE="${NAMENODE:-amazon_reviews_namenode}"
BASE="/user/bigdata/amazon_reviews"

docker exec "$NAMENODE" hdfs dfs -rm -r -f "$BASE/silver" "$BASE/gold" "$BASE/quarantine" "$BASE/metrics" "$BASE/models" || true
bash hadoop/hdfs_init.sh
