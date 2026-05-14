# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Batch prediction with a saved MLlib PipelineModel."""

from __future__ import annotations

import argparse

from amazon_reviews_pipeline.common.spark_session import build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sentiment predictions for a Parquet dataset.")
    parser.add_argument("--model", default="hdfs://namenode:9000/user/bigdata/amazon_reviews/models/sentiment_classifier")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="/workspace/output/predictions/batch_predictions")
    args = parser.parse_args()

    from pyspark.ml import PipelineModel

    spark = build_spark_session("AmazonReviewsBatchPredict", enable_hive=False)
    model = PipelineModel.load(args.model)
    predictions = model.transform(spark.read.parquet(args.input))
    predictions.select("text", "prediction", "probability").write.mode("overwrite").csv(args.output, header=True)
    spark.stop()


if __name__ == "__main__":
    main()
