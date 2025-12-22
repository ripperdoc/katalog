from __future__ import annotations

import hashlib

from katalog.metadata import (
    DATA_KEY,
    FILE_SIZE,
    HASH_MD5,
    TIME_MODIFIED,
    get_metadata_id,
)
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    file_data_change_dependencies,
)
from katalog.models import Asset, Metadata, make_metadata


def _has_existing_hash(asset: Asset) -> bool:
    cached: list[Metadata] = list(getattr(asset, "_metadata_cache", []) or [])
    try:
        key_id: int | None = get_metadata_id(HASH_MD5)
    except Exception:
        key_id = None
    for md in cached:
        if md.removed:
            continue
        if key_id is not None:
            if int(getattr(md, "metadata_key_id", -1)) == key_id:
                return True
        elif getattr(md, "key", None) == HASH_MD5:
            return True
    return False


class MD5HashProcessor(Processor):
    dependencies = frozenset(
        {DATA_KEY, FILE_SIZE, TIME_MODIFIED} | file_data_change_dependencies
    )
    outputs = frozenset({HASH_MD5})

    def should_run(
        self,
        asset: Asset,
        changes: set[str] | None,
    ) -> bool:
        change_set = set(changes or [])
        if change_set and HASH_MD5 in change_set:
            # Source already supplied the hash during this snapshot.
            return False
        if not _has_existing_hash(asset):
            return True
        if DATA_KEY in change_set:
            return True
        if FILE_SIZE in change_set or TIME_MODIFIED in change_set:
            return True
        return False

    async def run(self, asset: Asset, changes: set[str] | None) -> ProcessorResult:
        d = asset.data
        if d is None:
            raise ValueError("AssetRecord does not have a data accessor")
        hash_md5 = hashlib.md5()

        offset = 0
        chunk_size = 8192

        while True:
            chunk = await d.read(offset, chunk_size)
            if not chunk:
                break
            hash_md5.update(chunk)
            offset += len(chunk)

        return ProcessorResult(
            metadata=[make_metadata(HASH_MD5, hash_md5.hexdigest(), self.provider.id)]
        )
