# Big Data Pipeline Report Outline

## 1. Introduction

Business problem: customer review intelligence for e-commerce decision-making.

Selected dataset: Amazon Reviews 2023 from McAuley Lab.

Dataset justification: the official dataset contains 571.54M reviews across 33 product domains, which satisfies the assignment's public `>5 GB` Big Data requirement.

## 2. Environment Setup

Implementation mode: local Docker Compose.

Services:

- Hadoop NameNode and DataNode
- YARN ResourceManager and NodeManager
- Hive Metastore and HiveServer2
- Spark Master and Worker

Screenshots to include:

- Docker containers running
- Hadoop NameNode UI
- Spark Master UI
- Hive Beeline connection

## 3. HDFS Ingestion

Raw data path:

```text
/user/bigdata/landing/amazon_reviews/reviews
```

Pipeline stage:

```text
make ingest INPUT=data/raw/Office_Products.jsonl
```

## 4. Hive Schema

Database:

```text
ecommerce_reviews
```

Tables:

- `raw_reviews`
- `cleaned_reviews`

Schema module:

```text
hive/ddl/00_legacy_create_tables.sql
```

## 5. Spark Processing

Spark job:

```text
src/amazon_reviews_pipeline/processing/bronze_to_silver.py
```

Transformations:

- Remove rows missing `rating` or `text`
- Validate ratings between 1 and 5
- Create binary sentiment label
- Convert millisecond timestamp to review date
- Write cleaned Parquet data to HDFS

## 6. HiveQL Analysis

SQL module:

```text
hive/queries/02_business_insights.sql
```

Analysis outputs:

- Review count by sentiment
- Average rating by verified purchase status
- Category-level review volume and ratings
- Top reviewed products

## 7. Spark MLlib Model

Spark job:

```text
src/amazon_reviews_pipeline/ml/train_sentiment_model.py
```

Model:

- Tokenizer
- StopWordsRemover
- HashingTF
- IDF
- LogisticRegression

Metrics:

- Accuracy
- Weighted precision
- Weighted recall
- F1 score
- Confusion matrix counts

## 8. Cloud Alternative

Production alternative:

```text
Amazon S3 > AWS Glue Data Catalog > EMR Serverless Spark/Hive > Athena/QuickSight > Model Outputs
```

Docker was selected for the assignment implementation because it is reproducible, easier to screenshot, and does not require cloud account configuration.

## 9. GitHub Replication Instructions

Include:

- Docker installation prerequisite
- `make run INPUT=data/raw/Office_Products.jsonl`
- individual stage commands from the Makefile, if needed
