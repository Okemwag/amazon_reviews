#!/usr/bin/env bash
set -euo pipefail

python3 --version
uv --version
docker --version
docker compose version

mkdir -p data/raw data/sample logs output/charts output/metrics output/models output/predictions output/reports
test -f .env || cp .env.example .env

echo "Project bootstrap complete."
echo "Next steps:"
echo "  1. Put Office_Products.jsonl under data/raw/"
echo "  2. Run make start"
echo "  3. Run make check"
echo "  4. Run make pipeline INPUT=data/raw/Office_Products.jsonl"
