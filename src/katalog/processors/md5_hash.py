from __future__ import annotations

import hashlib
from typing import Any, Optional

from katalog.db import Database
from katalog.processors.base import Processor, file_data_changed
from katalog.models import HASH_MD5, FileRecord, Metadata, make_metadata


def _has_existing_hash(database: Optional[Database], record: FileRecord) -> bool:
    if not database:
        return False
    existing = database.get_metadata_for_file(
        record.id,
        source_id=record.source_id,
        metadata_key=HASH_MD5,
    )
    return bool(existing)


class MD5HashProcessor(Processor):
    PLUGIN_ID = "dev.katalog.processor.md5_hash"
    dependencies = frozenset()  # No dependencies, runs on any record
    outputs = frozenset({HASH_MD5})

    def should_run(
        self,
        record: FileRecord,
        changes: set[str] | None,
        database: Database | None = None,
    ) -> bool:
        if changes and HASH_MD5 in changes:
            # Source already supplied the hash during this snapshot.
            return False
        if file_data_changed(self, record, changes):
            return True
        return not _has_existing_hash(database, record)

    async def run(self, record: FileRecord, changes: set[str] | None) -> list[Metadata]:
        d = record.data
        if d is None:
            raise ValueError("FileRecord does not have a data accessor")
        hash_md5 = hashlib.md5()

        offset = 0
        chunk_size = 8192

        while True:
            chunk = await d.read(offset, chunk_size)
            if not chunk:
                break
            hash_md5.update(chunk)
            offset += len(chunk)
        return [make_metadata(self.PLUGIN_ID, HASH_MD5, hash_md5.hexdigest())]
