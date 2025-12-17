from __future__ import annotations

import hashlib
from typing import Any, Optional

from katalog.metadata import HASH_MD5
from katalog.processors.base import Processor, ProcessorResult, file_data_changed
from katalog.models import Asset, make_metadata


def _has_existing_hash(asset: Asset) -> bool:
    existing = database.get_metadata_for_file(
        asset.id,
        provider_id=asset.provider_id,
        metadata_key=HASH_MD5,
    )
    return bool(existing)


class MD5HashProcessor(Processor):
    PLUGIN_ID = "dev.katalog.processor.md5_hash"
    dependencies = frozenset()  # No dependencies, runs on any record
    outputs = frozenset({HASH_MD5})

    def should_run(
        self,
        asset: Asset,
        changes: set[str] | None,
    ) -> bool:
        if changes and HASH_MD5 in changes:
            # Source already supplied the hash during this snapshot.
            return False
        if file_data_changed(self, asset, changes):
            return True
        return not _has_existing_hash(asset)

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
        provider_id = getattr(self, "provider_id", asset.provider_id)

        return ProcessorResult(
            metadata=[make_metadata(provider_id, HASH_MD5, hash_md5.hexdigest())]
        )
