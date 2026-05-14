# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Feature engineering helpers for review analytics and ML."""

from __future__ import annotations

from datetime import datetime, timezone


def sentiment_label(rating: float | int | None) -> int | None:
    if rating is None:
        return None
    rating_value = float(rating)
    if rating_value >= 4:
        return 1
    if rating_value <= 2:
        return 0
    return 2


def rating_class(rating: float | int | None) -> str | None:
    label = sentiment_label(rating)
    if label == 1:
        return "positive"
    if label == 0:
        return "negative"
    if label == 2:
        return "neutral"
    return None


def word_count(text: str | None) -> int:
    return len(str(text or "").split())


def helpful_vote_bucket(votes: int | None) -> str:
    value = max(int(votes or 0), 0)
    if value == 0:
        return "none"
    if value < 5:
        return "low"
    if value < 25:
        return "medium"
    return "high"


def review_date_parts(timestamp_ms: int | None) -> dict[str, int | str | None]:
    if timestamp_ms is None:
        return {"review_date": None, "review_year": None, "review_month": None}
    dt = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
    return {"review_date": dt.date().isoformat(), "review_year": dt.year, "review_month": dt.month}


def add_spark_features(df):
    from pyspark.sql.functions import col, from_unixtime, length, month, size, split, to_date, when, year

    review_date = to_date(from_unixtime(col("timestamp") / 1000))
    return (
        df.withColumn("review_length", length(col("text")))
        .withColumn("word_count", size(split(col("text"), r"\s+")))
        .withColumn("helpful_vote_bucket", when(col("helpful_vote") == 0, "none").when(col("helpful_vote") < 5, "low").when(col("helpful_vote") < 25, "medium").otherwise("high"))
        .withColumn("verified_purchase_int", when(col("verified_purchase") == True, 1).otherwise(0))
        .withColumn("sentiment_label", when(col("rating") >= 4, 1).when(col("rating") <= 2, 0).otherwise(2))
        .withColumn("rating_class", when(col("rating") >= 4, "positive").when(col("rating") <= 2, "negative").otherwise("neutral"))
        .withColumn("review_date", review_date)
        .withColumn("review_year", year(review_date))
        .withColumn("review_month", month(review_date))
    )
