# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

from amazon_reviews_pipeline.ml.train_rating_classifier import build_sentiment_pipeline


def test_build_sentiment_pipeline_has_expected_stages():
    pipeline = build_sentiment_pipeline(num_features=128, max_iter=1)
    stage_names = [stage.__class__.__name__ for stage in pipeline.getStages()]
    assert stage_names == [
        "StringIndexer",
        "RegexTokenizer",
        "StopWordsRemover",
        "HashingTF",
        "IDF",
        "LogisticRegression",
    ]
