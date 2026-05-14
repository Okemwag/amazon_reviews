# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from amazon_reviews_pipeline.common.config_loader import load_config
from amazon_reviews_pipeline.ml.evaluate_model import classification_metrics, confusion_matrix
from amazon_reviews_pipeline.ml.prepare_training_data import prepare_training_frame, split_train_test
from amazon_reviews_pipeline.ml.train_rating_classifier import build_sentiment_pipeline

CONFIG_PATH = "/workspace/configs/dev.toml"
MODEL_PATH = "hdfs://namenode:9000/user/bigdata/amazon_reviews/models/sentiment_classifier"
METRICS_PATH = Path("/workspace/output/reports/model_metrics.txt")
JSON_METRICS_PATH = Path("/workspace/output/metrics/ml_metrics.json")
PREDICTIONS_PATH = "/workspace/output/predictions/sample_predictions"


def evaluate(predictions, metric_name: str) -> float:
    evaluator = MulticlassClassificationEvaluator(
        labelCol="sentiment_label",
        predictionCol="prediction",
        metricName=metric_name,
    )
    return evaluator.evaluate(predictions)


def main() -> None:
    config = load_config(CONFIG_PATH)
    ml_config = config.get("ml", {})
    spark = (
        SparkSession.builder.appName("AmazonReviewSentimentModel")
        .enableHiveSupport()
        .getOrCreate()
    )

    model_path = config.get("hdfs", {}).get("sentiment_model_dir", MODEL_PATH)
    reviews = spark.table("ecommerce_reviews.cleaned_reviews")
    training_year_min = int(ml_config.get("training_year_min", 0))
    if training_year_min:
        reviews = reviews.filter(f"review_year >= {training_year_min}")

    df = prepare_training_frame(reviews, include_neutral=False)
    seed = int(ml_config.get("seed", 42))
    max_training_rows = int(ml_config.get("max_training_rows", 0))

    source_count = None
    if max_training_rows > 0:
        df = df.limit(max_training_rows)
    else:
        source_count = df.count()

    training_count = df.count()
    if training_count < 20:
        train_df = df
        test_df = df
    else:
        train_df, test_df = split_train_test(
            df,
            train_fraction=float(ml_config.get("train_fraction", 0.8)),
            seed=seed,
        )

    pipeline = build_sentiment_pipeline(
        num_features=int(ml_config.get("hashing_features", 10000)),
        max_iter=int(ml_config.get("max_iter", 10)),
    )
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    metrics = classification_metrics(predictions)
    metrics["confusion_matrix"] = confusion_matrix(predictions)
    if source_count is not None:
        metrics["source_count"] = source_count
    metrics["training_sample_count"] = training_count
    metrics["train_count"] = train_df.count()
    metrics["test_count"] = test_df.count()

    model.write().overwrite().save(model_path)
    predictions.select(
        "text",
        "sentiment_label",
        "label",
        "prediction",
        F.col("probability").cast("string").alias("probability"),
    ).limit(1000).write.mode("overwrite").csv(PREDICTIONS_PATH, header=True)

    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with METRICS_PATH.open("w", encoding="utf-8") as handle:
        for name, value in metrics.items():
            handle.write(f"{name}: {value}\n")

    JSON_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_METRICS_PATH.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")

    print("Model metrics:")
    for name, value in metrics.items():
        print(f"{name}: {value}")
    print(f"Saved model to {model_path}")
    print(f"Saved metrics to {METRICS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
