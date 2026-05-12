from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession


MODEL_PATH = "hdfs://namenode:9000/user/bigdata/models/amazon_reviews/review_sentiment_model"
METRICS_PATH = Path("/workspace/output/reports/model_metrics.txt")


def evaluate(predictions, metric_name: str) -> float:
    evaluator = MulticlassClassificationEvaluator(
        labelCol="sentiment_label",
        predictionCol="prediction",
        metricName=metric_name,
    )
    return evaluator.evaluate(predictions)


def main() -> None:
    spark = (
        SparkSession.builder.appName("AmazonReviewSentimentModel")
        .enableHiveSupport()
        .getOrCreate()
    )

    df = (
        spark.table("ecommerce_reviews.cleaned_reviews")
        .select("text", "sentiment_label")
        .dropna()
    )

    if df.count() < 20:
        train_df = df
        test_df = df
    else:
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    pipeline = Pipeline(
        stages=[
            Tokenizer(inputCol="text", outputCol="words"),
            StopWordsRemover(inputCol="words", outputCol="filtered_words"),
            HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000),
            IDF(inputCol="raw_features", outputCol="features"),
            LogisticRegression(
                featuresCol="features",
                labelCol="sentiment_label",
                maxIter=10,
                regParam=0.01,
            ),
        ]
    )
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    metrics = {
        "accuracy": evaluate(predictions, "accuracy"),
        "weighted_precision": evaluate(predictions, "weightedPrecision"),
        "weighted_recall": evaluate(predictions, "weightedRecall"),
        "f1": evaluate(predictions, "f1"),
    }
    confusion_rows = predictions.groupBy("sentiment_label", "prediction").count()

    model.write().overwrite().save(MODEL_PATH)

    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with METRICS_PATH.open("w", encoding="utf-8") as handle:
        for name, value in metrics.items():
            handle.write(f"{name}: {value:.4f}\n")
        handle.write("\nConfusion matrix counts:\n")
        for row in confusion_rows.collect():
            handle.write(
                f"label={row['sentiment_label']}, "
                f"prediction={row['prediction']}, count={row['count']}\n"
            )

    print("Model metrics:")
    for name, value in metrics.items():
        print(f"{name}: {value:.4f}")
    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved metrics to {METRICS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
