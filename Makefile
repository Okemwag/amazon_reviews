CONFIG ?= configs/dev.toml
INPUT ?= data/raw/Office_Products.jsonl

.PHONY: start stop up down logs check upload pipeline run ingest tables transform quality analysis train validate test

start:
	docker compose up -d

stop:
	docker compose down

logs:
	docker compose logs -f

check:
	bash scripts/check_services.sh

upload:
	bash scripts/upload_to_hdfs.sh $(INPUT)

pipeline:
	bash scripts/run_full_pipeline.sh $(INPUT)

up:
	docker compose up -d

down:
	docker compose down

run:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline run --config $(CONFIG) --input $(INPUT)

ingest:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline ingest --config $(CONFIG) --input $(INPUT)

tables:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline tables --config $(CONFIG)

transform:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline transform --config $(CONFIG)

quality:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline quality --config $(CONFIG)

analysis:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline analysis --config $(CONFIG)

train:
	PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline train --config $(CONFIG)

validate:
	PYTHONPATH=src python3 -m py_compile src/amazon_reviews_pipeline/common/pipeline.py src/amazon_reviews_pipeline/processing/*.py src/amazon_reviews_pipeline/validation/*.py src/amazon_reviews_pipeline/ml/*.py

test:
	PYTHONPATH=src uv run pytest tests/
