#!/usr/bin/env bash
set -euo pipefail

bash scripts/upload_to_hdfs.sh "${1:-data/raw/Office_Products.jsonl}"
