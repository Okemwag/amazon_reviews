# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""CLI wrapper around the project ingestion stage."""

from __future__ import annotations

import argparse
from pathlib import Path

from amazon_reviews_pipeline.common.pipeline import PipelineConfig, ingest
from amazon_reviews_pipeline.ingestion.validate_raw_file import validate_raw_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate and upload raw reviews to HDFS.")
    parser.add_argument("--config", default="configs/dev.toml")
    parser.add_argument("--input", default="data/raw/Office_Products.jsonl")
    args = parser.parse_args()
    validate_raw_file(args.input)
    ingest(PipelineConfig.load(Path(args.config)), Path(args.input))


if __name__ == "__main__":
    main()
