# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Reusable MLlib text classification pipeline."""

from __future__ import annotations


def build_sentiment_pipeline(*, num_features: int = 10000, max_iter: int = 10, reg_param: float = 0.01):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover, StringIndexer

    return Pipeline(
        stages=[
            StringIndexer(inputCol="sentiment_label", outputCol="label", handleInvalid="skip"),
            RegexTokenizer(inputCol="text", outputCol="words", pattern=r"\W+"),
            StopWordsRemover(inputCol="words", outputCol="filtered_words"),
            HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=num_features),
            IDF(inputCol="raw_features", outputCol="features"),
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=max_iter, regParam=reg_param),
        ]
    )
