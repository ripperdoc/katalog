from __future__ import annotations

import math

DEFAULT_EMBEDDING_MODEL = "fast"
# Only "preset" is supported; "fastembed" is recognized to raise a helpful
# migration error. Kept as plain `str` since callers pass config values.
EmbeddingBackend = str


class EmbeddingError(RuntimeError):
    pass


async def embed_text_xberg(
    text: str,
    *,
    model: str = DEFAULT_EMBEDDING_MODEL,
    backend: EmbeddingBackend = "preset",
    normalize: bool = True,
    batch_size: int = 32,
    dim: int | None = None,
) -> list[float]:
    """Generate a text embedding through Xberg's chunk embedding path."""

    if not text.strip():
        if dim is None:
            return []
        return [0.0] * dim

    from xberg import (
        ChunkingConfig,
        EmbeddingConfig,
        EmbeddingModelType,
        ExtractionConfig,
        extract,
    )

    from katalog.utils.xberg import BytesExtractInput

    if backend == "fastembed":
        # The fastembed backend was removed when kreuzberg became xberg; the
        # built-in presets ("fast", etc.) cover the same models.
        raise EmbeddingError(
            "The 'fastembed' embedding backend was removed in xberg. "
            "Use backend='preset' with a preset name such as 'fast'."
        )
    if backend != "preset":
        raise EmbeddingError(f"Unknown embedding backend {backend!r}; only 'preset' is supported.")
    embedding_model = EmbeddingModelType.preset(model)

    result = await extract(
        input=BytesExtractInput(
            kind="bytes",
            bytes=text.encode("utf-8"),
            mime_type="text/plain",
        ),
        config=ExtractionConfig(
            chunking=ChunkingConfig(
                max_characters=max(32, len(text) + 1),
                overlap=0,
                embedding=EmbeddingConfig(
                    model=embedding_model,
                    normalize=normalize,
                    batch_size=batch_size,
                ),
            )
        ),
    )
    for doc in result.results:
        for chunk in doc.chunks or []:
            vector = chunk.embedding
            if isinstance(vector, list) and vector:
                return _resize_vector([float(v) for v in vector], dim=dim)

    raise EmbeddingError(
        "Xberg returned no embedding. Verify embedding model availability."
    )


def _resize_vector(vector: list[float], *, dim: int | None) -> list[float]:
    if dim is None:
        return _normalize_vector(vector)
    if dim <= 0:
        raise ValueError("dim must be > 0")
    if len(vector) > dim:
        resized = vector[:dim]
    elif len(vector) < dim:
        resized = [*vector, *([0.0] * (dim - len(vector)))]
    else:
        resized = vector
    return _normalize_vector(resized)


def _normalize_vector(vector: list[float]) -> list[float]:
    if not vector:
        return []

    norm = math.sqrt(sum(v * v for v in vector))
    if norm <= 0.0:
        return vector
    return [v / norm for v in vector]
