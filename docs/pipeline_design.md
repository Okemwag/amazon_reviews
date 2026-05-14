# Pipeline Design

The project implements a local big-data pipeline for Amazon Office Products reviews.

```text
Local JSONL
  -> HDFS bronze
  -> Hive raw external table
  -> Spark bronze-to-silver cleaning
  -> HDFS quarantine for invalid records
  -> Hive cleaned external table
  -> Spark/Hive analytics
  -> Gold Parquet outputs
  -> Spark MLlib sentiment model
```

The code is organized under `src/amazon_reviews_pipeline/`:

- `common`: configuration, logging, Spark session, HDFS helpers.
- `ingestion`: local validation, schema inference, HDFS upload wrapper.
- `validation`: schema, null, duplicate, and data-quality checks.
- `processing`: bronze-to-silver, silver-to-gold, text cleaning, feature engineering.
- `analytics`: summary statistics, insights, benchmark jobs.
- `ml`: training data prep, MLlib training, evaluation, prediction.
- `monitoring`: job metrics and audit helpers.
