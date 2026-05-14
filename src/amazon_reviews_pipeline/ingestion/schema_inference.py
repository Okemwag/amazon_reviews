# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Infer and persist schema metadata for the raw dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from amazon_reviews_pipeline.common.spark_session import build_spark_session


def infer_schema(input_path: str, output_path: str = "/workspace/output/metadata/schema.json") -> dict[str, object]:
    spark = build_spark_session("AmazonReviewsSchemaInference", enable_hive=False)
    df = spark.read.json(input_path)
    row_count = df.count()
    schema = json.loads(df.schema.json())
    df.show(5, truncate=80)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps({"row_count": row_count, "schema": schema}, indent=2) + "\n", encoding="utf-8")
    spark.stop()
    return {"row_count": row_count, "schema": schema, "output_path": output_path}


def main() -> None:
    parser = argparse.ArgumentParser(description="Infer Spark schema for a JSONL dataset.")
    parser.add_argument("--input", default="/workspace/data/raw/Office_Products.jsonl")
    parser.add_argument("--output", default="/workspace/output/metadata/schema.json")
    args = parser.parse_args()
    print(json.dumps(infer_schema(args.input, args.output), indent=2))


if __name__ == "__main__":
    main()
