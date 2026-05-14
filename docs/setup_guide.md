# Setup Guide

## Prerequisites

- Docker with Docker Compose.
- Python 3.11 or newer.
- `uv` for reproducible local test execution.

## Bootstrap

```bash
bash scripts/bootstrap_project.sh
make start
make check
```

Place the full Amazon Reviews file at:

```text
data/raw/Office_Products.jsonl
```

Then run:

```bash
make pipeline INPUT=data/raw/Office_Products.jsonl
```

## Verification

```bash
make test
make quality
make analysis
make train
```
