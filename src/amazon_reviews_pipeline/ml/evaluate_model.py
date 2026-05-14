# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Model evaluation utilities."""

from __future__ import annotations

import json
from pathlib import Path


def classification_metrics(predictions, *, label_col: str = "label", prediction_col: str = "prediction") -> dict[str, float]:
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

    metrics: dict[str, float] = {}
    for metric_name in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator = MulticlassClassificationEvaluator(labelCol=label_col, predictionCol=prediction_col, metricName=metric_name)
        metrics[metric_name] = float(evaluator.evaluate(predictions))
    metrics["roc_auc"] = float(BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction").evaluate(predictions))
    return metrics


def confusion_matrix(predictions, *, label_col: str = "label", prediction_col: str = "prediction") -> list[dict[str, int | float]]:
    return [
        {"label": row[label_col], "prediction": row[prediction_col], "count": row["count"]}
        for row in predictions.groupBy(label_col, prediction_col).count().collect()
    ]


def write_metrics(metrics: dict, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    print("Evaluation is run by train_sentiment_model.py after predictions are generated.")


if __name__ == "__main__":
    main()
