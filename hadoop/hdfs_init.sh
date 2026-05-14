#!/usr/bin/env bash
set -euo pipefail

NAMENODE="${NAMENODE:-amazon_reviews_namenode}"
BASE="/user/bigdata/amazon_reviews"

docker exec "$NAMENODE" hdfs dfs -mkdir -p \
  "$BASE/bronze/office_products" \
  "$BASE/silver/office_products_cleaned" \
  "$BASE/gold" \
  "$BASE/quarantine/invalid_reviews" \
  "$BASE/models" \
  "$BASE/metrics" \
  /user/bigdata/landing/amazon_reviews/reviews \
  /user/bigdata/processed/amazon_reviews/reviews_cleaned \
  /user/bigdata/manifests/amazon_reviews

docker exec "$NAMENODE" hdfs dfs -chmod -R 777 /user/bigdata
docker exec "$NAMENODE" hdfs dfs -ls "$BASE"
