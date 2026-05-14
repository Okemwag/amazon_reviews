# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from amazon_reviews_pipeline.validation.duplicate_checks import duplicate_keys, has_duplicates
from amazon_reviews_pipeline.validation.null_checks import is_missing, missing_value_counts


def test_null_rating_is_invalid():
    counts = missing_value_counts([{"rating": None, "text": "ok"}], ["rating", "text"])
    assert counts["rating"] == 1
    assert counts["text"] == 0
    assert is_missing(None)


def test_duplicate_rows_are_detected():
    records = [
        {"user_id": "u1", "asin": "a1", "timestamp": 1, "text": "same"},
        {"user_id": "u1", "asin": "a1", "timestamp": 1, "text": "same"},
    ]
    assert has_duplicates(records, ["user_id", "asin", "timestamp", "text"])
    assert duplicate_keys(records, ["user_id", "asin", "timestamp", "text"]) == [("u1", "a1", 1, "same")]
