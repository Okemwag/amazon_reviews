# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, length, lit, lower, month, regexp_replace, size, split, to_date, trim, when, year
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from amazon_reviews_pipeline.common.config_loader import load_config
from amazon_reviews_pipeline.common.logger import get_logger, log_job


CLEAN_TABLE = "ecommerce_reviews.cleaned_reviews"
DEFAULT_CONFIG = "/workspace/configs/dev.toml"
RAW_PATH = "hdfs://namenode:9000/user/bigdata/landing/amazon_reviews/reviews"
OUTPUT_PATH = "hdfs://namenode:9000/user/bigdata/processed/amazon_reviews/reviews_cleaned"
QUARANTINE_PATH = "hdfs://namenode:9000/user/bigdata/amazon_reviews/quarantine/invalid_reviews"
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


def invalid_condition():
    return (
        col("rating").isNull()
        | (col("rating") < 1)
        | (col("rating") > 5)
        | col("text").isNull()
        | (length(trim(col("text"))) == 0)
        | col("asin").isNull()
        | col("user_id").isNull()
        | col("timestamp").isNull()
        | (col("helpful_vote") < 0)
        | col("verified_purchase").isNull()
    )


def add_invalid_reason(df):
    return df.withColumn(
        "invalid_reason",
        when(col("rating").isNull(), "missing_rating")
        .when((col("rating") < 1) | (col("rating") > 5), "invalid_rating")
        .when(col("text").isNull() | (length(trim(col("text"))) == 0), "missing_review_text")
        .when(col("asin").isNull(), "missing_product_id")
        .when(col("user_id").isNull(), "missing_user_id")
        .when(col("timestamp").isNull(), "invalid_timestamp")
        .when(col("helpful_vote") < 0, "negative_helpful_vote")
        .when(col("verified_purchase").isNull(), "invalid_verified_purchase")
        .otherwise("unknown"),
    )


def main() -> None:
    logger = get_logger("bronze_to_silver")
    config = load_config(DEFAULT_CONFIG)
    hdfs = config.get("hdfs", {})
    raw_path = hdfs.get("bronze_reviews_dir", RAW_PATH)
    # Keep compatibility with the existing Hive table location.
    raw_path = raw_path if raw_path.startswith("hdfs://") else RAW_PATH
    output_path = hdfs.get("silver_reviews_dir", hdfs.get("processed_reviews_dir", OUTPUT_PATH))
    quarantine_path = hdfs.get("quarantine_dir", QUARANTINE_PATH)

    spark = (
        SparkSession.builder.appName("AmazonReviewsTransform")
        .config("spark.sql.shuffle.partitions", config.get("spark", {}).get("shuffle_partitions", "48"))
        .config("spark.sql.parquet.compression.codec", config.get("spark", {}).get("parquet_compression", "snappy"))
        .enableHiveSupport()
        .getOrCreate()
    )

    with log_job(logger, "bronze_to_silver", input_path=raw_path, output_path=output_path):
        raw_df = spark.read.schema(RAW_SCHEMA).json(raw_path)
        invalid_df = add_invalid_reason(raw_df.filter(invalid_condition()))
        duplicate_df = raw_df.dropna(subset=["user_id", "asin", "timestamp", "text"]).groupBy(
            "user_id", "asin", "timestamp", "text"
        ).count().filter(col("count") > 1)

        invalid_df.write.mode("overwrite").parquet(quarantine_path)

        valid_df = raw_df.filter(~invalid_condition()).dropDuplicates(["user_id", "asin", "timestamp", "text"])
        review_date = to_date(from_unixtime(col("timestamp") / 1000))
        clean_df = (
            valid_df.withColumn("title", trim(col("title")))
            .withColumn("text", lower(trim(regexp_replace(regexp_replace(col("text"), r"<[^>]+>", " "), r"\s+", " "))))
            .withColumn("sentiment_label", when(col("rating") >= 4, 1).when(col("rating") <= 2, 0).otherwise(2))
            .withColumn("rating_class", when(col("rating") >= 4, "positive").when(col("rating") <= 2, "negative").otherwise("neutral"))
            .withColumn("review_length", length(col("text")))
            .withColumn("word_count", size(split(col("text"), r"\s+")))
            .withColumn("helpful_vote_bucket", when(col("helpful_vote") == 0, "none").when(col("helpful_vote") < 5, "low").when(col("helpful_vote") < 25, "medium").otherwise("high"))
            .withColumn("verified_purchase_int", when(col("verified_purchase") == True, 1).otherwise(0))
            .withColumn("review_date", review_date)
            .withColumn("review_year", year(review_date))
            .withColumn("review_month", month(review_date))
            .withColumnRenamed("timestamp", "review_ts")
            .select(
                "rating",
                "title",
                "text",
                "asin",
                "parent_asin",
                "user_id",
                "review_ts",
                "helpful_vote",
                "verified_purchase",
                "category",
                "sentiment_label",
                "rating_class",
                "review_length",
                "word_count",
                "helpful_vote_bucket",
                "verified_purchase_int",
                col("review_date").cast("string").alias("review_date"),
                "review_year",
                "review_month",
            )
        )

        clean_df.write.mode("overwrite").partitionBy("review_year", "review_month").parquet(output_path)
        spark.sql(f"REFRESH TABLE {CLEAN_TABLE}")

        raw_count = raw_df.count()
        clean_count = clean_df.count()
        invalid_count = invalid_df.count()
        duplicate_count = duplicate_df.count()
        logger.info("input_rows=%s output_rows=%s invalid_rows=%s duplicate_keys=%s", raw_count, clean_count, invalid_count, duplicate_count)
        print("Cleaned review counts by sentiment label:")
        clean_df.groupBy("sentiment_label").count().show()
        print(f"Wrote cleaned reviews to {output_path}")
        print(f"Wrote invalid reviews to {quarantine_path}")

    spark.stop()


if __name__ == "__main__":
    main()
