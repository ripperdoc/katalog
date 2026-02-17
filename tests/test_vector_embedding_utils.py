from __future__ import annotations

import pytest

from katalog.vectors.embedding import _resize_vector


def test_resize_vector_truncates_and_normalizes() -> None:
    vec = _resize_vector([1.0, 2.0, 3.0, 4.0], dim=2)
    assert len(vec) == 2
    norm_sq = sum(v * v for v in vec)
    assert norm_sq == pytest.approx(1.0, rel=1e-6)


def test_resize_vector_pads_and_normalizes() -> None:
    vec = _resize_vector([1.0, 2.0], dim=4)
    assert len(vec) == 4
    norm_sq = sum(v * v for v in vec)
    assert norm_sq == pytest.approx(1.0, rel=1e-6)
