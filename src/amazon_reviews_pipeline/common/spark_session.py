# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Reusable SparkSession builder."""

from __future__ import annotations


def build_spark_session(
    app_name: str,
    *,
    master: str | None = None,
    enable_hive: bool = True,
    shuffle_partitions: int = 48,
    parquet_compression: str = "snappy",
    local_mode: bool = False,
):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if local_mode:
        builder = builder.master(master or "local[*]")
    elif master:
        builder = builder.master(master)
    builder = (
        builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", parquet_compression)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if enable_hive:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()
