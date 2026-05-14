# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, length, sum as spark_sum, trim, when
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType


CLEAN_TABLE = "ecommerce_reviews.cleaned_reviews"
RAW_PATH = "hdfs://namenode:9000/user/bigdata/landing/amazon_reviews/reviews"
REPORT_PATH = Path("/workspace/output/reports/quality_report.txt")
METRICS_PATH = Path("/workspace/output/metrics/data_quality_metrics.json")
RAW_SCHEMA = StructType(
    [
        StructField("rating", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("helpful_vote", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("category", StringType(), True),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("AmazonReviewsQualityChecks")
        .enableHiveSupport()
        .getOrCreate()
    )

    raw_df = spark.read.schema(RAW_SCHEMA).json(RAW_PATH)
    clean_df = spark.table(CLEAN_TABLE)

    raw_count = raw_df.count()
    clean_count = clean_df.count()
    raw_invalid_rating_count = raw_df.filter((col("rating") < 1) | (col("rating") > 5)).count()
    clean_invalid_rating_count = clean_df.filter((col("rating") < 1) | (col("rating") > 5)).count()
    duplicate_records = raw_df.dropna(subset=["user_id", "asin", "timestamp", "text"]).groupBy(
        "user_id", "asin", "timestamp", "text"
    ).count().filter(col("count") > 1).count()
    invalid_timestamp = raw_df.filter(col("timestamp").isNull() | (col("timestamp") <= 0)).count()
    negative_helpful_votes = raw_df.filter(col("helpful_vote") < 0).count()
    missing_or_empty_text = raw_df.filter(col("text").isNull() | (length(trim(col("text"))) == 0)).count()
    missing_required = clean_df.select(
        spark_sum(when(col("rating").isNull(), 1).otherwise(0)).alias("missing_rating"),
        spark_sum(when(col("text").isNull() | (length(trim(col("text"))) == 0), 1).otherwise(0)).alias("missing_text"),
        spark_sum(when(col("asin").isNull(), 1).otherwise(0)).alias("missing_asin"),
        spark_sum(when(col("user_id").isNull(), 1).otherwise(0)).alias("missing_user_id"),
        spark_sum(when(col("review_ts").isNull(), 1).otherwise(0)).alias("missing_timestamp"),
        spark_sum(when(col("helpful_vote") < 0, 1).otherwise(0)).alias("negative_helpful_vote"),
    ).collect()[0]

    sentiment_counts = {
        str(row["sentiment_label"]): row["count"]
        for row in clean_df.groupBy("sentiment_label").agg(count("*").alias("count")).collect()
    }

    failures = []
    if raw_count == 0:
        failures.append("raw_reviews table is empty")
    if clean_count == 0:
        failures.append("cleaned_reviews table is empty")
    if clean_invalid_rating_count > 0:
        failures.append(f"cleaned_reviews contains {clean_invalid_rating_count} invalid ratings")
    for field in missing_required.asDict():
        if missing_required[field] > 0:
            failures.append(f"cleaned_reviews contains {missing_required[field]} rows for {field}")

    metrics = {
        "total_records": raw_count,
        "valid_records": clean_count,
        "invalid_records": max(raw_count - clean_count, 0),
        "null_review_text": missing_or_empty_text,
        "invalid_rating": raw_invalid_rating_count,
        "clean_invalid_rating": clean_invalid_rating_count,
        "duplicate_records": duplicate_records,
        "invalid_timestamp": invalid_timestamp,
        "negative_helpful_vote": negative_helpful_votes,
        "status": "failed" if failures else "passed",
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as handle:
        handle.write(f"raw_count={raw_count}\n")
        handle.write(f"clean_count={clean_count}\n")
        handle.write(f"invalid_rating_count={raw_invalid_rating_count}\n")
        handle.write(f"raw_invalid_rating_count={raw_invalid_rating_count}\n")
        handle.write(f"clean_invalid_rating_count={clean_invalid_rating_count}\n")
        for key, value in missing_required.asDict().items():
            handle.write(f"{key}={value}\n")
        handle.write(f"sentiment_counts={sentiment_counts}\n")
        handle.write(f"status={'failed' if failures else 'passed'}\n")
        for failure in failures:
            handle.write(f"failure={failure}\n")

    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")

    if failures:
        raise RuntimeError("; ".join(failures))

    print(f"Quality checks passed. Report: {REPORT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
