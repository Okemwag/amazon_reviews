# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Product-level analytics queries."""

from __future__ import annotations


def top_products(records: list[dict], *, limit: int = 10) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for record in records:
        asin = record.get("asin")
        if asin:
            counts[str(asin)] = counts.get(str(asin), 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]


def product_summary_sql(limit: int = 10) -> str:
    return f"""
SELECT asin, ROUND(AVG(rating), 2) AS average_rating, COUNT(*) AS review_count, SUM(helpful_vote) AS helpful_votes
FROM ecommerce_reviews.cleaned_reviews
GROUP BY asin
ORDER BY review_count DESC, helpful_votes DESC
LIMIT {int(limit)}
""".strip()
