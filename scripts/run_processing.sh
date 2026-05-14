#!/usr/bin/env bash
set -euo pipefail

make transform
docker exec amazon_reviews_spark_master bash -c '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/amazon_reviews_pipeline/processing/silver_to_gold.py'
