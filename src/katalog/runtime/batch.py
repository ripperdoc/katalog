from __future__ import annotations

import os
from typing import AsyncIterator, Iterable, Iterator, TypeVar


T = TypeVar("T")


DEFAULT_BATCH_SIZE = 1000


def get_batch_size(env_key: str = "KATALOG_BATCH_SIZE", default: int = DEFAULT_BATCH_SIZE) -> int:
    raw = os.environ.get(env_key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def iter_batches(items: Iterable[T], batch_size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


async def iter_batches_async(
    iterator: AsyncIterator[T], batch_size: int
) -> AsyncIterator[list[T]]:
    batch: list[T] = []
    async for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
