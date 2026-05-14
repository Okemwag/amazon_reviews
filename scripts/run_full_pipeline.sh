#!/usr/bin/env bash
set -euo pipefail

INPUT="${1:-data/raw/Office_Products.jsonl}"
CONFIG="${CONFIG:-configs/dev.toml}"

PYTHONPATH=src python3 -m amazon_reviews_pipeline.ingestion.validate_raw_file --input "$INPUT"
PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline run --config "$CONFIG" --input "$INPUT"
docker exec amazon_reviews_spark_master bash -c '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/amazon_reviews_pipeline/processing/silver_to_gold.py'
docker exec amazon_reviews_spark_master bash -c '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /workspace/src/amazon_reviews_pipeline/analytics/generate_summary_stats.py'
