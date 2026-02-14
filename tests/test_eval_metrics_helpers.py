from __future__ import annotations

import pytest

from katalog.analyzers.eval_metrics import _build_format_metrics_rows


def test_build_format_metrics_rows_groups_and_normalizes_scores() -> None:
    rows = [
        {
            "mime": "application/pdf",
            "similarity": 80.0,
            "completeness": 90.0,
            "unique_word_ratio": 0.5,
        },
        {
            "mime": "application/pdf",
            "similarity": 100.0,
            "completeness": 80.0,
            "unique_word_ratio": 0.7,
        },
        {
            "mime": "text/plain",
            "similarity": None,
            "completeness": None,
            "unique_word_ratio": 0.2,
        },
    ]

    out = _build_format_metrics_rows(rows)
    assert len(out) == 2

    pdf = next(item for item in out if item["mime"] == "application/pdf")
    txt = next(item for item in out if item["mime"] == "text/plain")

    assert pdf["similarity_score"] == pytest.approx(0.9)
    assert pdf["completeness"] == pytest.approx(0.85)
    assert pdf["unique_word_ratio"] == pytest.approx(0.6)

    assert txt["similarity_score"] == 0.0
    assert txt["completeness"] == 0.0
    assert txt["unique_word_ratio"] == pytest.approx(0.2)
