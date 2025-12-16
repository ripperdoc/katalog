from __future__ import annotations

import hashlib
from typing import cast

import pytest

from katalog.db import Database
from katalog.models import AssetRecord, HASH_MD5
from katalog.processors.md5_hash import MD5HashProcessor
from tests.utils.fakes import DatabaseStub, MemoryAccessor


def make_record(record_id: str = "file-1") -> AssetRecord:
    return AssetRecord(id=record_id, provider_id="source-1", canonical_uri="uri://file")


def test_should_run_skips_when_hash_already_present(database_stub: DatabaseStub):
    processor = MD5HashProcessor()
    record = make_record()
    assert (
        processor.should_run(record, {HASH_MD5}, cast(Database, database_stub)) is False
    )


def test_should_run_when_hash_missing(database_stub: DatabaseStub):
    database_stub.set_method("get_metadata_for_file", lambda *args, **kwargs: [])
    processor = MD5HashProcessor()
    record = make_record()
    should_run = processor.should_run(
        record, changes=None, database=cast(Database, database_stub)
    )
    assert should_run is True


@pytest.mark.asyncio
async def test_run_computes_expected_hash():
    processor = MD5HashProcessor()
    record = make_record()
    payload = b"hello world"
    record.attach_accessor(MemoryAccessor(payload))

    result = await processor.run(record, changes=None)

    assert len(result.metadata) == 1
    metadata = result.metadata[0]
    assert metadata.key == HASH_MD5
    assert metadata.value == hashlib.md5(payload).hexdigest()
    assert metadata.provider_id == record.provider_id
