from __future__ import annotations

import hashlib

import pytest

from katalog.metadata import FILE_SIZE, HASH_MD5, TIME_MODIFIED
from katalog.models import Asset, Provider, ProviderType
from katalog.processors.md5_hash import MD5HashProcessor
from tests.utils.fakes import MemoryAccessor


@pytest.fixture(autouse=True)
def patch_metadata_ids(monkeypatch):
    monkeypatch.setattr("katalog.processors.md5_hash.get_metadata_id", lambda key: 1)
    monkeypatch.setattr("katalog.metadata.get_metadata_id", lambda key: 1)
    monkeypatch.setattr("katalog.models.get_metadata_id", lambda key: 1)


def make_provider() -> Provider:
    return Provider(id=1, name="p", plugin_id="p", type=ProviderType.SOURCE)


def make_record() -> Asset:
    return Asset(
        provider_id=1,
        canonical_id="cid",
        canonical_uri="uri://file",
    )


def with_hash_cache(asset: Asset) -> Asset:
    class _MD:
        metadata_key_id = 1
        removed = False

    asset._metadata_cache = [_MD()]
    return asset


def test_should_run_skips_when_hash_already_present_and_no_change():
    processor = MD5HashProcessor(provider=make_provider())
    record = with_hash_cache(make_record())
    assert processor.should_run(record, set()) is False


def test_should_run_when_hash_missing():
    processor = MD5HashProcessor(provider=make_provider())
    record = make_record()
    should_run = processor.should_run(record, changes=None)
    assert should_run is True


def test_runs_when_fingerprint_changed_even_with_existing_hash():
    processor = MD5HashProcessor(provider=make_provider())
    record = with_hash_cache(make_record())
    assert processor.should_run(record, {FILE_SIZE}) is True
    assert processor.should_run(record, {TIME_MODIFIED}) is True


@pytest.mark.asyncio
async def test_run_computes_expected_hash():
    processor = MD5HashProcessor(provider=make_provider())
    record = make_record()
    payload = b"hello world"
    record.attach_accessor(MemoryAccessor(payload))

    result = await processor.run(record, changes=None)

    assert len(result.metadata) == 1
    metadata = result.metadata[0]
    assert metadata.value_text == hashlib.md5(payload).hexdigest()
