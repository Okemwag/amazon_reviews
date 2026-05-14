# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Prepare sentiment training data."""

from __future__ import annotations


def prepare_training_frame(df, *, include_neutral: bool = False):
    data = df.select("text", "sentiment_label").dropna()
    if not include_neutral:
        data = data.filter("sentiment_label != 2")
    return data


def split_train_test(df, *, train_fraction: float = 0.8, seed: int = 42):
    if not 0 < train_fraction <= 1:
        raise ValueError("train_fraction must be in the range (0, 1]")
    if train_fraction == 1:
        return df, df
    return df.randomSplit([train_fraction, 1 - train_fraction], seed=seed)
