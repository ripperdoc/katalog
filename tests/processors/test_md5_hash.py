from __future__ import annotations

import hashlib

import pytest

from katalog.metadata import HASH_MD5
from katalog.models import Asset
from katalog.processors.md5_hash import MD5HashProcessor
from tests.utils.fakes import MemoryAccessor


def make_record(asset_id: str = "file-1") -> Asset:
    return Asset(id=asset_id, provider_id="source-1", canonical_uri="uri://file")


def test_should_run_skips_when_hash_already_present():
    processor = MD5HashProcessor()
    record = make_record()
    assert processor.should_run(record, {HASH_MD5}) is False


def test_should_run_when_hash_missing():
    processor = MD5HashProcessor()
    record = make_record()
    should_run = processor.should_run(record, changes=None)
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
    # assert metadata.provider_id == record.provider_id
