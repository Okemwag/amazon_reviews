# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Shared names and defaults used by the Amazon Reviews pipeline."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "dev.toml"
DEFAULT_YAML_CONFIG_PATH = PROJECT_ROOT / "configs" / "dev.yaml"

RAW_REVIEW_COLUMNS = [
    "rating",
    "title",
    "text",
    "asin",
    "parent_asin",
    "user_id",
    "timestamp",
    "helpful_vote",
    "verified_purchase",
    "category",
]

CLEAN_REVIEW_COLUMNS = [
    "rating",
    "title",
    "text",
    "asin",
    "parent_asin",
    "user_id",
    "review_ts",
    "helpful_vote",
    "verified_purchase",
    "category",
    "sentiment_label",
    "rating_class",
    "review_length",
    "word_count",
    "helpful_vote_bucket",
    "verified_purchase_int",
    "review_date",
    "review_year",
    "review_month",
]

REQUIRED_RAW_FIELDS = ["rating", "text", "asin", "user_id", "timestamp"]
DEDUPLICATION_KEYS = ["user_id", "asin", "timestamp", "text"]

DEFAULT_HDFS_BASE = "hdfs://namenode:9000/user/bigdata/amazon_reviews"
DEFAULT_BRONZE_PATH = f"{DEFAULT_HDFS_BASE}/bronze/office_products"
DEFAULT_SILVER_PATH = f"{DEFAULT_HDFS_BASE}/silver/office_products_cleaned"
DEFAULT_GOLD_PATH = f"{DEFAULT_HDFS_BASE}/gold"
DEFAULT_QUARANTINE_PATH = f"{DEFAULT_HDFS_BASE}/quarantine/invalid_reviews"
DEFAULT_MODELS_PATH = f"{DEFAULT_HDFS_BASE}/models/sentiment_classifier"
DEFAULT_METRICS_PATH = f"{DEFAULT_HDFS_BASE}/metrics"

LOCAL_METRICS_DIR = PROJECT_ROOT / "output" / "metrics"
LOCAL_REPORTS_DIR = PROJECT_ROOT / "output" / "reports"
LOCAL_CHARTS_DIR = PROJECT_ROOT / "output" / "charts"
LOCAL_PREDICTIONS_DIR = PROJECT_ROOT / "output" / "predictions"
