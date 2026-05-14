# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Compare Spark DataFrame and Spark SQL runtimes for common aggregations."""

from __future__ import annotations

import json
from pathlib import Path
from time import perf_counter

from amazon_reviews_pipeline.common.spark_session import build_spark_session


def timed(name: str, fn) -> dict[str, float | str]:
    start = perf_counter()
    fn()
    return {"task": name, "seconds": round(perf_counter() - start, 4)}


def main() -> None:
    spark = build_spark_session("AmazonReviewsSparkVsHiveBenchmark")
    df = spark.table("ecommerce_reviews.cleaned_reviews")
    results = [
        timed("spark_count_total", lambda: df.count()),
        timed("spark_group_by_rating", lambda: df.groupBy("rating").count().collect()),
        timed("spark_group_by_verified_purchase", lambda: df.groupBy("verified_purchase").count().collect()),
        timed("spark_group_by_year_month", lambda: df.groupBy("review_year", "review_month").count().collect()),
        timed("sql_count_total", lambda: spark.sql("SELECT COUNT(*) FROM ecommerce_reviews.cleaned_reviews").collect()),
        timed("sql_group_by_rating", lambda: spark.sql("SELECT rating, COUNT(*) FROM ecommerce_reviews.cleaned_reviews GROUP BY rating").collect()),
        timed("sql_group_by_verified_purchase", lambda: spark.sql("SELECT verified_purchase, COUNT(*) FROM ecommerce_reviews.cleaned_reviews GROUP BY verified_purchase").collect()),
        timed("sql_group_by_year_month", lambda: spark.sql("SELECT review_year, review_month, COUNT(*) FROM ecommerce_reviews.cleaned_reviews GROUP BY review_year, review_month").collect()),
    ]
    output = Path("/workspace/output/metrics/spark_vs_hive_benchmark.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
