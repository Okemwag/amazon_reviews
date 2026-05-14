# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from amazon_reviews_pipeline.processing.feature_engineering import helpful_vote_bucket, rating_class, review_date_parts, sentiment_label, word_count


def test_rating_labels():
    assert sentiment_label(5) == 1
    assert sentiment_label(1) == 0
    assert sentiment_label(3) == 2
    assert rating_class(5) == "positive"
    assert rating_class(1) == "negative"
    assert rating_class(3) == "neutral"


def test_word_count():
    assert word_count("one two three") == 3
    assert word_count("") == 0


def test_helpful_vote_bucket():
    assert helpful_vote_bucket(0) == "none"
    assert helpful_vote_bucket(3) == "low"
    assert helpful_vote_bucket(10) == "medium"
    assert helpful_vote_bucket(30) == "high"


def test_review_date_parts():
    parts = review_date_parts(1693526400000)
    assert parts["review_year"] == 2023
    assert parts["review_month"] == 9
