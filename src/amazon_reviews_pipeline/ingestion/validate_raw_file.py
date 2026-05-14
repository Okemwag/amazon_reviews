# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Validate local Amazon Reviews JSON Lines files before HDFS upload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from amazon_reviews_pipeline.common.constants import REQUIRED_RAW_FIELDS


def validate_raw_file(path: str | Path, *, sample_size: int = 1000) -> dict[str, object]:
    input_path = Path(path)
    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")
    if input_path.stat().st_size == 0:
        raise ValueError(f"Input file is empty: {input_path}")
    if not input_path.name.endswith(".jsonl"):
        raise ValueError("Input file must use the .jsonl extension")

    sampled = 0
    missing_fields: dict[str, int] = {field: 0 for field in REQUIRED_RAW_FIELDS}
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if sampled >= sample_size:
                break
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"Line {line_number} is not a JSON object")
            for field in REQUIRED_RAW_FIELDS:
                if field not in record or record[field] in (None, ""):
                    missing_fields[field] += 1
            sampled += 1

    return {
        "path": str(input_path),
        "byte_size": input_path.stat().st_size,
        "sampled_rows": sampled,
        "missing_fields": missing_fields,
        "status": "passed",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a local Amazon Reviews JSONL file.")
    parser.add_argument("--input", default="data/raw/Office_Products.jsonl")
    parser.add_argument("--sample-size", type=int, default=1000)
    args = parser.parse_args()
    print(json.dumps(validate_raw_file(args.input, sample_size=args.sample_size), indent=2))


if __name__ == "__main__":
    main()
