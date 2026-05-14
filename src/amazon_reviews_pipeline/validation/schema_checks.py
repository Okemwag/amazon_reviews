# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Schema validation helpers."""

from __future__ import annotations

from amazon_reviews_pipeline.common.constants import CLEAN_REVIEW_COLUMNS, RAW_REVIEW_COLUMNS


def missing_columns(actual_columns: list[str], expected_columns: list[str]) -> list[str]:
    actual = set(actual_columns)
    return [column for column in expected_columns if column not in actual]


def validate_columns(actual_columns: list[str], expected_columns: list[str]) -> None:
    missing = missing_columns(actual_columns, expected_columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def validate_raw_schema(actual_columns: list[str]) -> None:
    validate_columns(actual_columns, RAW_REVIEW_COLUMNS)


def validate_clean_schema(actual_columns: list[str]) -> None:
    validate_columns(actual_columns, CLEAN_REVIEW_COLUMNS)
