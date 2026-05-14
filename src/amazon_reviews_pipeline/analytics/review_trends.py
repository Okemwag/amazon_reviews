# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Time-series review analytics."""

from __future__ import annotations


def monthly_trends_sql() -> str:
    return """
SELECT review_year, review_month, COUNT(*) AS review_count, ROUND(AVG(rating), 2) AS average_rating
FROM ecommerce_reviews.cleaned_reviews
GROUP BY review_year, review_month
ORDER BY review_year, review_month
""".strip()
