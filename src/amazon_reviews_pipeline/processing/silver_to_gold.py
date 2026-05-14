# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Create business-ready gold Parquet outputs from cleaned reviews."""

from __future__ import annotations

from amazon_reviews_pipeline.common.config_loader import load_config
from amazon_reviews_pipeline.common.spark_session import build_spark_session

CONFIG_PATH = "/workspace/configs/dev.toml"
CLEAN_TABLE = "ecommerce_reviews.cleaned_reviews"


def write_gold_outputs(spark, gold_dir: str) -> None:
    clean = spark.table(CLEAN_TABLE)
    clean.groupBy("asin", "category").agg(
        {"rating": "avg", "asin": "count", "helpful_vote": "sum"}
    ).write.mode("overwrite").parquet(f"{gold_dir}/product_summary")
    clean.groupBy("rating").count().write.mode("overwrite").parquet(f"{gold_dir}/rating_distribution")
    clean.groupBy("sentiment_label", "rating_class").count().write.mode("overwrite").parquet(f"{gold_dir}/sentiment_distribution")
    clean.groupBy("review_year", "review_month").count().write.mode("overwrite").partitionBy("review_year").parquet(f"{gold_dir}/monthly_review_trends")
    clean.groupBy("verified_purchase").agg({"rating": "avg", "asin": "count"}).write.mode("overwrite").parquet(f"{gold_dir}/verified_purchase_summary")
    clean.filter("sentiment_label != 2").select("text", "sentiment_label").write.mode("overwrite").parquet(f"{gold_dir}/ml_training_data")


def main() -> None:
    config = load_config(CONFIG_PATH)
    spark = build_spark_session("AmazonReviewsSilverToGold", shuffle_partitions=int(config.get("spark", {}).get("shuffle_partitions", 48)))
    gold_dir = config.get("hdfs", {}).get("gold_dir", "hdfs://namenode:9000/user/bigdata/amazon_reviews/gold")
    write_gold_outputs(spark, gold_dir)
    print(f"Wrote gold outputs under {gold_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
