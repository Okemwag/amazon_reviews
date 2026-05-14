# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Null and empty-value validation helpers."""

from __future__ import annotations


def missing_value_counts(records: list[dict], fields: list[str]) -> dict[str, int]:
    counts = {field: 0 for field in fields}
    for record in records:
        for field in fields:
            value = record.get(field)
            if value is None or value == "":
                counts[field] += 1
    return counts


def is_missing(value: object) -> bool:
    return value is None or value == ""
