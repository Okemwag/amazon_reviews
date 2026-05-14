#!/usr/bin/env bash
set -euo pipefail

INPUT="${1:-/workspace/data/raw/Office_Products.jsonl}"
NAMENODE="${NAMENODE:-amazon_reviews_namenode}"
TARGET="/user/bigdata/amazon_reviews/bronze/office_products/Office_Products.jsonl"
LEGACY_TARGET="/user/bigdata/landing/amazon_reviews/reviews/Office_Products.jsonl"

bash hadoop/hdfs_init.sh
docker exec "$NAMENODE" hdfs dfs -put -f "$INPUT" "$TARGET"
docker exec "$NAMENODE" hdfs dfs -put -f "$INPUT" "$LEGACY_TARGET"
docker exec "$NAMENODE" hdfs dfs -ls /user/bigdata/amazon_reviews/bronze/office_products
docker exec "$NAMENODE" hdfs dfs -du -h /user/bigdata/amazon_reviews/bronze/office_products
