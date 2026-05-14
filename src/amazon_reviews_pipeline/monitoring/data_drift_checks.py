# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Lightweight data drift checks for review distributions."""

from __future__ import annotations


def distribution_delta(current: dict[str, int], baseline: dict[str, int]) -> dict[str, int]:
    keys = set(current) | set(baseline)
    return {key: current.get(key, 0) - baseline.get(key, 0) for key in sorted(keys)}


def max_absolute_delta(delta: dict[str, int]) -> int:
    return max((abs(value) for value in delta.values()), default=0)
