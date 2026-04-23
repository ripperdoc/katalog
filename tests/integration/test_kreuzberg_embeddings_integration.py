from __future__ import annotations

import os
from pathlib import Path

import pytest

from katalog.vectors.embedding import embed_text_kreuzberg


if os.getenv("KATALOG_RUN_KREUZBERG_EMBEDDING_IT") != "1":
    pytest.skip(
        "Set KATALOG_RUN_KREUZBERG_EMBEDDING_IT=1 to run Kreuzberg embedding integration tests.",
        allow_module_level=True,
    )


def _ensure_ort_dylib_path() -> None:
    if os.getenv("ORT_DYLIB_PATH"):
        return
    try:
        import onnxruntime
    except Exception:
        return

    root = Path(onnxruntime.__file__).parent
    for pattern in ("libonnxruntime*.so*", "libonnxruntime*.dylib", "onnxruntime*.dll"):
        for candidate in root.rglob(pattern):
            name = candidate.name.lower()
            if "providers" in name or "shared" in name:
                continue
            os.environ["ORT_DYLIB_PATH"] = str(candidate)
            return


_ensure_ort_dylib_path()


@pytest.mark.asyncio
async def test_kreuzberg_embedding_api_returns_vector() -> None:
    kreuzberg = pytest.importorskip("kreuzberg")

    if hasattr(kreuzberg, "embed"):
        vectors = await kreuzberg.embed(
            ["Katalog integration test"],
            config=kreuzberg.EmbeddingConfig(
                model=kreuzberg.EmbeddingModelType.preset("fast"),
                normalize=True,
            )
        )
        assert len(vectors) == 1
        assert isinstance(vectors[0], list)
        assert len(vectors[0]) > 0
        return

    result = await kreuzberg.extract_bytes(
        b"Katalog integration test",
        mime_type="text/plain",
        config=kreuzberg.ExtractionConfig(
            chunking=kreuzberg.ChunkingConfig(
                max_chars=64,
                max_overlap=0,
                embedding=kreuzberg.EmbeddingConfig(
                    model=kreuzberg.EmbeddingModelType.preset("fast"),
                    normalize=True,
                ),
            )
        ),
    )

    assert result.chunks
    vector = result.chunks[0].embedding
    assert isinstance(vector, list)
    assert len(vector) > 0


@pytest.mark.asyncio
async def test_katalog_wrapper_returns_vector() -> None:
    vector = await embed_text_kreuzberg("Katalog integration test")

    assert isinstance(vector, list)
    assert len(vector) > 0
