from __future__ import annotations

import math
from typing import Literal


DEFAULT_EMBEDDING_MODEL = "fast"
EmbeddingBackend = Literal["preset", "fastembed"]


class EmbeddingError(RuntimeError):
    pass


async def embed_text_kreuzberg(
    text: str,
    *,
    model: str = DEFAULT_EMBEDDING_MODEL,
    backend: EmbeddingBackend = "preset",
    normalize: bool = True,
    batch_size: int = 32,
    dim: int | None = None,
) -> list[float]:
    """Generate a text embedding through Kreuzberg's chunk embedding path."""

    if not text.strip():
        if dim is None:
            return []
        return [0.0] * dim

    from kreuzberg import (
        ChunkingConfig,
        EmbeddingConfig,
        EmbeddingModelType,
        ExtractionConfig,
        extract_bytes,
    )

    if backend == "fastembed":
        embedding_model = EmbeddingModelType.fastembed(model)
    else:
        embedding_model = EmbeddingModelType.preset(model)

    result = await extract_bytes(
        text.encode("utf-8"),
        mime_type="text/plain",
        config=ExtractionConfig(
            chunking=ChunkingConfig(
                max_chars=max(32, len(text) + 1),
                max_overlap=0,
                embedding=EmbeddingConfig(
                    model=embedding_model,
                    normalize=normalize,
                    batch_size=batch_size,
                ),
            )
        ),
    )
    for chunk in result.chunks or []:
        vector = getattr(chunk, "embedding", None)
        if isinstance(vector, list) and vector:
            return _resize_vector([float(v) for v in vector], dim=dim)

    raise EmbeddingError(
        "Kreuzberg returned no embedding. Verify ONNX runtime/model availability."
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
