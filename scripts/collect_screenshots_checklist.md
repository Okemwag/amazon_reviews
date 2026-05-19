# Screenshot Checklist

Use the files in `output/evidence/` and `output/charts/` as screenshot-ready proof. For web UI screenshots, open the URLs below while the Docker stack is running.

## Live UI Screenshots

- Docker containers running: screenshot `output/evidence/01_docker_containers_running.txt` or run `docker compose ps`.
- Hadoop NameNode UI: open `http://localhost:9870`.
- HDFS browser showing raw upload: in NameNode UI browse to `/user/bigdata/landing/amazon_reviews/reviews`.
- Spark Master UI: open `http://localhost:8080`.
- Spark job evidence: use Spark Master completed applications, or rerun a job such as `make quality` while viewing `http://localhost:8080`.
- YARN ResourceManager UI, if needed: open `http://localhost:8088`.

## Terminal Evidence Screenshots

- HDFS raw file uploaded: `output/evidence/02_hdfs_raw_file_uploaded.txt`.
- HDFS project directories: `output/evidence/03_hdfs_directory_tree.txt`.
- Silver Parquet output in HDFS: `output/evidence/04_silver_parquet_output_in_hdfs.txt`.
- Gold Parquet output in HDFS: `output/evidence/05_gold_parquet_output_in_hdfs.txt`.
- Saved model path in HDFS: `output/evidence/06_model_path_in_hdfs.txt`.
- Predictions sample: `output/evidence/07_predictions_sample_from_hdfs.txt`.
- Hive database/table creation and schema output: `output/evidence/08_hive_database_tables_output.txt`.
- Hive SELECT query results: `output/evidence/09_hive_select_query_results.txt`.
- Data quality check result: `output/evidence/10_quality_metrics_file.txt`.
- Data quality report: `output/evidence/11_quality_report_file.txt`.
- ML final metrics: `output/evidence/12_ml_metrics_file.txt`.
- ML training/evaluation report: `output/evidence/13_model_metrics_report.txt`.
- Spark schema output: `output/evidence/spark_schema_output.txt`.
- Spark cleaned row count: `output/evidence/spark_cleaned_row_count.txt`.
- Spark vs Hive timing comparison: `output/evidence/16_spark_vs_hive_timing_comparison.json`.

## Chart Screenshots

- Rating distribution chart: `output/charts/rating_distribution.svg`.
- Sentiment distribution chart: `output/charts/sentiment_distribution.svg`.
- Confusion matrix: `output/charts/confusion_matrix.svg`.
- ROC/AUC summary: `output/charts/roc_auc_summary.svg`.
- Model evaluation metrics chart: `output/charts/model_evaluation_metrics.svg`.
- Feature importance: not applicable to the current Logistic Regression pipeline; see `output/evidence/feature_importance_note.txt`.
