from __future__ import annotations

import os

import pytest

from katalog.vectors.embedding import embed_text_xberg


if os.getenv("KATALOG_RUN_XBERG_EMBEDDING_IT") != "1":
    pytest.skip(
        "Set KATALOG_RUN_XBERG_EMBEDDING_IT=1 to run Xberg embedding integration tests.",
        allow_module_level=True,
    )


@pytest.mark.asyncio
async def test_xberg_embedding_api_returns_vector() -> None:
    xberg = pytest.importorskip("xberg")

    result = await xberg.extract(
        input=xberg.ExtractInput(
            kind="bytes",
            bytes=b"Katalog integration test",
            mime_type="text/plain",
        ),
        config=xberg.ExtractionConfig(
            chunking=xberg.ChunkingConfig(
                max_characters=64,
                overlap=0,
                embedding=xberg.EmbeddingConfig(
                    model=xberg.EmbeddingModelType.preset("fast"),
                    normalize=True,
                ),
            )
        ),
    )

    assert result.results
    doc = result.results[0]
    assert doc.chunks
    vector = doc.chunks[0].embedding
    assert isinstance(vector, list)
    assert len(vector) > 0


@pytest.mark.asyncio
async def test_katalog_wrapper_returns_vector() -> None:
    vector = await embed_text_xberg("Katalog integration test")

    assert isinstance(vector, list)
    assert len(vector) > 0
