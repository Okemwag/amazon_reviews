# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from amazon_reviews_pipeline.processing.text_cleaning import clean_text, is_valid_review_text


def test_clean_text_lowercases_and_normalizes_whitespace():
    assert clean_text("  Great   PRODUCT  ") == "great product"


def test_clean_text_removes_html():
    assert clean_text("<b>Great</b> item") == "great item"


def test_empty_review_text_is_invalid():
    assert not is_valid_review_text("   ")
    assert not is_valid_review_text(None)
