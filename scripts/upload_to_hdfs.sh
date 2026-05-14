#!/usr/bin/env bash
set -euo pipefail

INPUT="${1:-data/raw/Office_Products.jsonl}"
CONFIG="${CONFIG:-configs/dev.toml}"

PYTHONPATH=src python3 -m amazon_reviews_pipeline.ingestion.validate_raw_file --input "$INPUT"
PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline ingest --config "$CONFIG" --input "$INPUT"
