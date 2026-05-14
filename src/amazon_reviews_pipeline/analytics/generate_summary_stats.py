# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Generate summary statistics from cleaned reviews."""

from __future__ import annotations

import json
from pathlib import Path

from amazon_reviews_pipeline.common.spark_session import build_spark_session


def summary_stats(records: list[dict]) -> dict[str, float | int]:
    ratings = [float(record["rating"]) for record in records if record.get("rating") is not None]
    return {
        "review_count": len(records),
        "rated_review_count": len(ratings),
        "average_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0.0,
    }


def main() -> None:
    spark = build_spark_session("AmazonReviewsSummaryStats")
    df = spark.table("ecommerce_reviews.cleaned_reviews")
    stats = {
        "total_reviews": df.count(),
        "average_rating": df.agg({"rating": "avg"}).collect()[0][0],
        "sentiment_distribution": {str(row["sentiment_label"]): row["count"] for row in df.groupBy("sentiment_label").count().collect()},
    }
    output = Path("/workspace/output/metrics/summary_stats.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(stats, indent=2) + "\n", encoding="utf-8")
    df.groupBy("rating").count().orderBy("rating").coalesce(1).write.mode("overwrite").csv(
        "/workspace/output/metrics/rating_distribution_csv",
        header=True,
    )
    print(json.dumps(stats, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
