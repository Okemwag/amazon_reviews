# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Verified purchase analytics."""

from __future__ import annotations


def verified_purchase_sql() -> str:
    return """
SELECT verified_purchase, COUNT(*) AS review_count, ROUND(AVG(rating), 2) AS average_rating, SUM(helpful_vote) AS helpful_votes
FROM ecommerce_reviews.cleaned_reviews
GROUP BY verified_purchase
ORDER BY review_count DESC
""".strip()
