"""Tests for asset upsert and processing flow caching."""

import pytest

from katalog.metadata import FILE_PATH
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    Snapshot,
    OpStatus,
)
from tests.utils.upsert_helpers import UpsertFixture, ctx, md  # noqa: F401


@pytest.mark.asyncio
async def test_upsert_reuses_canonical_asset(ctx: UpsertFixture):
    # Existing asset already created in fixture
    existing = ctx.asset
    original_created = existing.created_snapshot_id

    # New snapshot for this run
    snap = await Snapshot.create(provider=ctx.provider, status=OpStatus.COMPLETED)

    # New Asset instance with the same canonical_id should reuse the row
    new_asset = Asset(
        provider=ctx.provider,
        canonical_id=existing.canonical_id,
        canonical_uri="file:///updated",
        created_snapshot=snap,
        last_snapshot=snap,
    )

    meta = md(FILE_PATH, "/tmp/new")
    meta.provider = ctx.provider
    meta.snapshot = snap
    meta.asset = new_asset

    await new_asset.save_record(snapshot=snap)
    change_set = MetadataChangeSet(
        loaded=await new_asset.load_metadata(), staged=[meta]
    )
    changes = await change_set.persist(asset=new_asset, snapshot=snap)

    assert new_asset.id == existing.id
    assert new_asset.created_snapshot_id == original_created
    assert new_asset.last_snapshot_id == snap.id
    assert FILE_PATH in changes


@pytest.mark.asyncio
async def test_upsert_uses_metadata_cache(ctx: UpsertFixture):
    calls = 0

    orig_fetch = ctx.asset.fetch_related

    async def wrapped(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await orig_fetch(*args, **kwargs)

    ctx.asset.fetch_related = wrapped  # type: ignore

    # First upsert populates cache
    await ctx.upsert(provider_id=0, snapshot_id=1, metas=[md(FILE_PATH, "/tmp/one")])
    # Second save should use cache and skip fetch_related
    await ctx.upsert(provider_id=0, snapshot_id=2, metas=[md(FILE_PATH, "/tmp/two")])

    assert calls == 1
    rows = await ctx.fetch_rows(FILE_PATH)
    assert [r.value_text for r in rows] == ["/tmp/one", "/tmp/two"]
