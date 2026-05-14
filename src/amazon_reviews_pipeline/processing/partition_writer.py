# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Partitioned Spark writers."""

from __future__ import annotations


def write_parquet(df, output_path: str, partition_cols: list[str] | None = None, mode: str = "overwrite") -> None:
    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(output_path)
