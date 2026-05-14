# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

import pytest

from amazon_reviews_pipeline.common.constants import RAW_REVIEW_COLUMNS
from amazon_reviews_pipeline.validation.schema_checks import missing_columns, validate_raw_schema


def test_missing_columns_detected():
    assert missing_columns(["rating", "text"], ["rating", "text", "asin"]) == ["asin"]


def test_validate_raw_schema_passes():
    validate_raw_schema(RAW_REVIEW_COLUMNS)


def test_validate_raw_schema_fails():
    with pytest.raises(ValueError):
        validate_raw_schema(["rating"])
