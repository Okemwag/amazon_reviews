# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Review text cleaning functions shared by Spark jobs and unit tests."""

from __future__ import annotations

import html
import re

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


def clean_text(value: str | None, *, lowercase: bool = True) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = HTML_TAG_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text.lower() if lowercase else text


def is_valid_review_text(value: str | None) -> bool:
    return bool(clean_text(value))


def spark_clean_text(column):
    from pyspark.sql.functions import lower, regexp_replace, trim

    no_html = regexp_replace(column, r"<[^>]+>", " ")
    normalized = regexp_replace(no_html, r"\s+", " ")
    return lower(trim(normalized))
