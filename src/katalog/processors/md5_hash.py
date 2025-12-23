from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

from katalog.metadata import (
    DATA_KEY,
    FILE_SIZE,
    HASH_MD5,
    TIME_MODIFIED,
)
from katalog.processors.base import Processor, ProcessorResult
from katalog.models import Asset, make_metadata, MetadataChangeSet


class MD5HashProcessor(Processor):
    dependencies = frozenset({DATA_KEY, FILE_SIZE, TIME_MODIFIED})
    outputs = frozenset({HASH_MD5})

    def should_run(self, asset: Asset, change_set: MetadataChangeSet) -> bool:
        changes = change_set.changed_keys()
        if HASH_MD5 in changes:
            return False
        if DATA_KEY in changes:
            return True
        if FILE_SIZE in changes or TIME_MODIFIED in changes:
            return True
        if HASH_MD5 not in change_set.current():
            return True
        return False

    async def run(self, asset: Asset, change_set: MetadataChangeSet) -> ProcessorResult:
        d = asset.data
        if d is None:
            raise ValueError("Asset does not have a data accessor")

        # If the accessor exposes a local path, hash it in a thread to leverage GIL release.
        if hasattr(d, "path"):
            digest = await asyncio.to_thread(_hash_file_path, Path(d.path))  # type: ignore
        else:
            digest = await _hash_stream_async(d)

        return ProcessorResult(
            metadata=[make_metadata(HASH_MD5, digest, self.provider.id)]
        )


def _hash_file_path(path: Path) -> str:
    hash_md5 = hashlib.md5()
    chunk_size = 8192

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


async def _hash_stream_async(accessor) -> str:
    hash_md5 = hashlib.md5()
    offset = 0
    chunk_size = 8192

    while True:
        chunk = await accessor.read(offset, chunk_size)
        if not chunk:
            break
        hash_md5.update(chunk)
        offset += len(chunk)

    return hash_md5.hexdigest()
