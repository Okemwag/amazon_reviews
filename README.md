# Amazon Office Reviews Big Data Pipeline

Production-style local data pipeline for Amazon Reviews 2023 using Hadoop HDFS,
Hive, Spark, and Spark MLlib.

The project has one orchestrated entry point:

```bash
PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline run --input data/raw/Office_Products.jsonl
```

or:

```bash
make run INPUT=data/raw/Office_Products.jsonl
```

## Architecture

```text
data/raw/*.jsonl
  -> HDFS landing zone
  -> Hive raw_reviews external table
  -> Spark cleaning and feature engineering
  -> Hive cleaned_reviews external table
  -> Spark quality checks
  -> Hive analytical outputs
  -> Spark MLlib sentiment model and metrics
```

## Project Layout

```text
configs/dev.toml                  Runtime configuration.
src/amazon_reviews_pipeline/      Pipeline package.
hive/ddl/                         Hive DDL.
hive/queries/                     Hive analytical SQL.
data/raw/                         Local raw Amazon Reviews files.
output/metrics/                   Generated ingestion manifests and metrics.
output/reports/                   Local quality reports.
docker-compose.yml                Local Hadoop, Hive, and Spark runtime.
```

Pipeline behavior should be changed in the package under
`src/amazon_reviews_pipeline`.

## Run End To End

```bash
make run INPUT=data/raw/Office_Products.jsonl
```

This command starts the runtime, ingests the raw file into HDFS, writes an
ingestion manifest, creates Hive tables, runs the Spark transform, executes
quality checks, runs analysis queries, and trains the sentiment model.

Outputs:

```text
output/metrics/manifests/*.json
output/reports/quality_report.txt
output/reports/model_metrics.txt
```

## Run Individual Stages

```bash
make up
make ingest INPUT=data/raw/Office_Products.jsonl
make tables
make transform
make quality
make analysis
make train
```

The same stages are available through the Python module:

```bash
PYTHONPATH=src python3 -m amazon_reviews_pipeline.common.pipeline ingest --input data/raw/Office_Products.jsonl
```

## Runtime Services

- Hadoop NameNode: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- Spark Master: http://localhost:8080
- HiveServer2 JDBC: `jdbc:hive2://localhost:10000`

## Dataset

The selected dataset is Amazon Reviews 2023 from McAuley Lab.

- Official dataset page: https://amazon-reviews-2023.github.io/
- Hugging Face mirror: https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023

The current local raw file is:

```text
data/raw/Office_Products.jsonl
```

## Configuration

All paths and container names are configured in:

```text
configs/dev.toml
```

Use this file to change HDFS zones, Docker container names, Spark job paths, or
Hive SQL paths without editing orchestration code.
