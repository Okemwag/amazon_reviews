# Copyright (c) 2026 Maureen Chemutai.
# All rights reserved.
#
# This source code is proprietary and confidential. Unauthorized copying,
# modification, distribution, or use of this file, via any medium, is strictly
# prohibited without prior written permission from Maureen Chemutai.

"""Export small text and SVG artifacts that are useful for report screenshots."""

from __future__ import annotations

import json
from pathlib import Path
from xml.sax.saxutils import escape

from amazon_reviews_pipeline.common.spark_session import build_spark_session


OUTPUT_ROOT = Path("/workspace/output")
EVIDENCE_DIR = OUTPUT_ROOT / "evidence"
CHART_DIR = OUTPUT_ROOT / "charts"
METRICS_PATH = OUTPUT_ROOT / "metrics" / "ml_metrics.json"
CLEAN_TABLE = "ecommerce_reviews.cleaned_reviews"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def svg_document(width: int, height: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">\n'
        '<rect width="100%" height="100%" fill="#ffffff"/>\n'
        '<style>text{font-family:Arial,Helvetica,sans-serif;fill:#111827}'
        '.title{font-size:22px;font-weight:700}.label{font-size:13px}'
        '.small{font-size:11px;fill:#4b5563}.value{font-size:12px;font-weight:700}</style>\n'
        f"{body}\n</svg>\n"
    )


def write_bar_chart(path: Path, title: str, rows: list[tuple[str, int]], color: str = "#2563eb") -> None:
    width = 920
    height = 110 + len(rows) * 54
    max_value = max((value for _, value in rows), default=1)
    body = [f'<text x="30" y="40" class="title">{escape(title)}</text>']
    x = 220
    chart_width = 620
    for index, (label, value) in enumerate(rows):
        y = 82 + index * 54
        bar_width = int(chart_width * (value / max_value)) if max_value else 0
        body.append(f'<text x="30" y="{y + 18}" class="label">{escape(label)}</text>')
        body.append(f'<rect x="{x}" y="{y}" width="{chart_width}" height="24" fill="#e5e7eb" rx="3"/>')
        body.append(f'<rect x="{x}" y="{y}" width="{bar_width}" height="24" fill="{color}" rx="3"/>')
        body.append(f'<text x="{x + chart_width + 16}" y="{y + 18}" class="value">{value:,}</text>')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg_document(width, height, "\n".join(body)), encoding="utf-8")


def write_confusion_matrix(path: Path, matrix: list[dict[str, int | float]]) -> None:
    counts = {(int(row["label"]), int(row["prediction"])): int(row["count"]) for row in matrix}
    labels = [0, 1]
    max_value = max(counts.values(), default=1)
    body = ['<text x="30" y="40" class="title">Confusion Matrix</text>']
    body.append('<text x="250" y="76" class="label">Predicted negative</text>')
    body.append('<text x="465" y="76" class="label">Predicted positive</text>')
    body.append('<text x="68" y="155" class="label">Actual negative</text>')
    body.append('<text x="72" y="295" class="label">Actual positive</text>')
    for row_index, actual in enumerate(labels):
        for col_index, predicted in enumerate(labels):
            value = counts.get((actual, predicted), 0)
            intensity = 0.18 + 0.72 * (value / max_value)
            blue = int(255 - 120 * intensity)
            fill = f"rgb({blue},{blue + 25},255)"
            x = 230 + col_index * 220
            y = 100 + row_index * 140
            body.append(f'<rect x="{x}" y="{y}" width="170" height="110" fill="{fill}" stroke="#1f2937"/>')
            body.append(f'<text x="{x + 85}" y="{y + 58}" text-anchor="middle" class="title">{value:,}</text>')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg_document(720, 390, "\n".join(body)), encoding="utf-8")


def write_metrics_chart(path: Path, metrics: dict[str, float]) -> None:
    rows = [
        ("Accuracy", metrics.get("accuracy", 0.0)),
        ("Weighted precision", metrics.get("weightedPrecision", 0.0)),
        ("Weighted recall", metrics.get("weightedRecall", 0.0)),
        ("F1-score", metrics.get("f1", 0.0)),
        ("ROC AUC", metrics.get("roc_auc", 0.0)),
    ]
    width = 920
    height = 410
    body = ['<text x="30" y="40" class="title">Model Evaluation Metrics</text>']
    x = 250
    chart_width = 560
    for index, (label, value) in enumerate(rows):
        y = 82 + index * 58
        bar_width = int(chart_width * value)
        body.append(f'<text x="30" y="{y + 19}" class="label">{escape(label)}</text>')
        body.append(f'<rect x="{x}" y="{y}" width="{chart_width}" height="26" fill="#e5e7eb" rx="3"/>')
        body.append(f'<rect x="{x}" y="{y}" width="{bar_width}" height="26" fill="#059669" rx="3"/>')
        body.append(f'<text x="{x + chart_width + 16}" y="{y + 19}" class="value">{value:.3f}</text>')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg_document(width, height, "\n".join(body)), encoding="utf-8")


def write_roc_summary(path: Path, auc: float) -> None:
    # This is an AUC summary graphic, not a point-by-point ROC curve.
    width = 760
    height = 430
    body = [
        '<text x="30" y="40" class="title">ROC AUC Summary</text>',
        '<text x="30" y="70" class="small">Point-by-point ROC coordinates were not persisted; this summarizes the computed AUC.</text>',
        '<line x1="110" y1="340" x2="620" y2="340" stroke="#111827" stroke-width="2"/>',
        '<line x1="110" y1="340" x2="110" y2="100" stroke="#111827" stroke-width="2"/>',
        '<line x1="110" y1="340" x2="620" y2="100" stroke="#9ca3af" stroke-dasharray="6 5"/>',
        '<path d="M110 340 C185 230, 300 150, 620 105" fill="none" stroke="#dc2626" stroke-width="4"/>',
        '<text x="265" y="390" class="label">False positive rate</text>',
        '<text x="30" y="225" class="label" transform="rotate(-90 30,225)">True positive rate</text>',
        f'<text x="330" y="210" class="title">AUC = {auc:.3f}</text>',
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg_document(width, height, "\n".join(body)), encoding="utf-8")


def main() -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    CHART_DIR.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session("AmazonReviewsScreenshotEvidence")
    clean = spark.table(CLEAN_TABLE)

    # Spark prints schema to stdout, so collect a JSON schema too for a stable text file.
    write_text(EVIDENCE_DIR / "spark_schema_output.txt", clean._jdf.schema().treeString() + "\n")

    row_count = clean.count()
    write_text(EVIDENCE_DIR / "spark_cleaned_row_count.txt", f"cleaned_reviews row_count={row_count}\n")

    rating_rows = [(str(row["rating"]), int(row["count"])) for row in clean.groupBy("rating").count().orderBy("rating").collect()]
    sentiment_rows = [
        (f"{row['rating_class']} ({row['sentiment_label']})", int(row["count"]))
        for row in clean.groupBy("sentiment_label", "rating_class").count().orderBy("sentiment_label").collect()
    ]

    write_text(EVIDENCE_DIR / "rating_distribution.json", json.dumps(rating_rows, indent=2) + "\n")
    write_text(EVIDENCE_DIR / "sentiment_distribution.json", json.dumps(sentiment_rows, indent=2) + "\n")
    write_bar_chart(CHART_DIR / "rating_distribution.svg", "Rating Distribution", rating_rows, "#2563eb")
    write_bar_chart(CHART_DIR / "sentiment_distribution.svg", "Sentiment Distribution", sentiment_rows, "#7c3aed")

    sample_rows = spark.sql(
        """
        SELECT asin, rating, rating_class, verified_purchase, review_year, review_month
        FROM ecommerce_reviews.cleaned_reviews
        LIMIT 20
        """
    ).collect()
    sample_text = "\n".join(str(row.asDict()) for row in sample_rows) + "\n"
    write_text(EVIDENCE_DIR / "hive_select_query_results.txt", sample_text)

    if METRICS_PATH.exists():
        metrics = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
        write_confusion_matrix(CHART_DIR / "confusion_matrix.svg", metrics.get("confusion_matrix", []))
        write_metrics_chart(CHART_DIR / "model_evaluation_metrics.svg", metrics)
        write_roc_summary(CHART_DIR / "roc_auc_summary.svg", float(metrics.get("roc_auc", 0.0)))

    feature_note = (
        "Feature importance chart is not applicable for this run because the model is LogisticRegression "
        "with HashingTF features, not RandomForestClassifier. Hash buckets are not directly interpretable "
        "as original words.\n"
    )
    write_text(EVIDENCE_DIR / "feature_importance_note.txt", feature_note)

    spark.stop()
    print(f"Wrote screenshot evidence under {EVIDENCE_DIR}")
    print(f"Wrote charts under {CHART_DIR}")


if __name__ == "__main__":
    main()
