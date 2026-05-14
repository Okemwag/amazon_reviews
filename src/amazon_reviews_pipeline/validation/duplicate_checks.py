# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Duplicate review detection helpers."""

from __future__ import annotations


def duplicate_keys(records: list[dict], key_fields: list[str]) -> list[tuple]:
    seen: set[tuple] = set()
    duplicates: set[tuple] = set()
    for record in records:
        key = tuple(record.get(field) for field in key_fields)
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return sorted(duplicates)


def has_duplicates(records: list[dict], key_fields: list[str]) -> bool:
    return bool(duplicate_keys(records, key_fields))
