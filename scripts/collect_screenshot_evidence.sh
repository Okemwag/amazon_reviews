#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVIDENCE_DIR="$ROOT_DIR/output/evidence"
mkdir -p "$EVIDENCE_DIR"

run_and_save() {
  local name="$1"
  shift
  {
    printf '$'
    printf ' %q' "$@"
    printf '\n\n'
    "$@"
  } > "$EVIDENCE_DIR/$name"
}

run_and_save "01_docker_containers_running.txt" docker compose ps

run_and_save "02_hdfs_raw_file_uploaded.txt" \
  docker exec amazon_reviews_namenode hdfs dfs -ls -h /user/bigdata/landing/amazon_reviews/reviews

run_and_save "03_hdfs_directory_tree.txt" \
  docker exec amazon_reviews_namenode hdfs dfs -ls /user/bigdata/amazon_reviews

run_and_save "04_silver_parquet_output_in_hdfs.txt" \
  docker exec amazon_reviews_namenode hdfs dfs -ls /user/bigdata/processed/amazon_reviews/reviews_cleaned

run_and_save "05_gold_parquet_output_in_hdfs.txt" \
  docker exec amazon_reviews_namenode hdfs dfs -ls /user/bigdata/amazon_reviews/gold

run_and_save "06_model_path_in_hdfs.txt" \
  docker exec amazon_reviews_namenode hdfs dfs -ls -R /user/bigdata/amazon_reviews/models/sentiment_classifier

run_and_save "07_predictions_sample_from_hdfs.txt" \
  docker exec amazon_reviews_namenode bash -c "hdfs dfs -cat '/workspace/output/predictions/sample_predictions/part-*.csv' | head -30"

run_and_save "08_hive_database_tables_output.txt" \
  docker exec amazon_reviews_hive_server beeline -u jdbc:hive2://localhost:10000 -e "SHOW DATABASES; USE ecommerce_reviews; SHOW TABLES; DESCRIBE cleaned_reviews;"

run_and_save "09_hive_select_query_results.txt" \
  docker exec amazon_reviews_hive_server beeline -u jdbc:hive2://localhost:10000 -e "SET hive.fetch.task.conversion=more; USE ecommerce_reviews; SELECT asin, rating, rating_class, verified_purchase, review_year, review_month FROM cleaned_reviews LIMIT 10;"

run_and_save "10_quality_metrics_file.txt" \
  sed -n '1,220p' "$ROOT_DIR/output/metrics/data_quality_metrics.json"

run_and_save "11_quality_report_file.txt" \
  sed -n '1,220p' "$ROOT_DIR/output/reports/quality_report.txt"

run_and_save "12_ml_metrics_file.txt" \
  sed -n '1,220p' "$ROOT_DIR/output/metrics/ml_metrics.json"

run_and_save "13_model_metrics_report.txt" \
  sed -n '1,220p' "$ROOT_DIR/output/reports/model_metrics.txt"

run_and_save "14_generated_chart_files.txt" \
  find "$ROOT_DIR/output/charts" -maxdepth 1 -type f -print | sort

run_and_save "15_generated_evidence_files.txt" \
  find "$EVIDENCE_DIR" -maxdepth 1 -type f -print | sort

printf 'Screenshot evidence written to %s\n' "$EVIDENCE_DIR"
