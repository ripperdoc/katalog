from __future__ import annotations

import pytest

from katalog.metadata import FILE_NAME, FILE_PATH
from katalog.models import (
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Snapshot,
    make_metadata,
)
from tests.utils.pipeline_helpers import PipelineFixture


@pytest.mark.asyncio
async def test_persist_none_does_not_write_null_row_when_no_prior_value(
    pipeline_db,
) -> None:
    fx = await PipelineFixture.create()

    snapshot2 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    loaded = await fx.asset.load_metadata()
    staged = [make_metadata(FILE_PATH, None, provider_id=fx.provider.id)]

    cs = MetadataChangeSet(loaded=loaded, staged=staged)
    changed = await cs.persist(asset=fx.asset, snapshot=snapshot2)

    assert changed == set()

    key_id = staged[0].metadata_key_id
    assert (
        await Metadata.filter(
            asset_id=fx.asset.id, provider_id=fx.provider.id, metadata_key_id=key_id
        ).count()
        == 0
    )


@pytest.mark.asyncio
async def test_persist_none_clears_single_existing_value(pipeline_db) -> None:
    fx = await PipelineFixture.create()

    existing = fx.metadata(FILE_PATH, "/tmp/a")
    await existing.save()
    await fx.asset.load_metadata()

    snapshot2 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_PATH, None, provider_id=fx.provider.id)]
    cs = MetadataChangeSet(loaded=await fx.asset.load_metadata(), staged=staged)

    changed = await cs.persist(asset=fx.asset, snapshot=snapshot2)
    assert FILE_PATH in changed

    key_id = existing.metadata_key_id

    rows = await Metadata.filter(
        asset_id=fx.asset.id, provider_id=fx.provider.id, metadata_key_id=key_id
    ).order_by("id")
    assert len(rows) == 2
    assert {r.value_text for r in rows} == {"/tmp/a"}
    assert any(r.removed for r in rows)
    assert any(not r.removed for r in rows)

    # No NULL-value rows should be written for this key.
    assert (
        await Metadata.filter(
            asset_id=fx.asset.id,
            provider_id=fx.provider.id,
            metadata_key_id=key_id,
            value_text__isnull=True,
        ).count()
        == 0
    )

    # Current state should be empty (value was cleared).
    current = MetadataChangeSet(await fx.asset.load_metadata()).current(fx.provider.id)
    assert FILE_PATH not in current or current[FILE_PATH] == []


@pytest.mark.asyncio
async def test_persist_none_clears_multiple_existing_values(pipeline_db) -> None:
    fx = await PipelineFixture.create()

    a = fx.metadata(FILE_PATH, "/tmp/a")
    b = fx.metadata(FILE_PATH, "/tmp/b")
    await a.save()
    await b.save()
    await fx.asset.load_metadata()

    snapshot2 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_PATH, None, provider_id=fx.provider.id)]
    cs = MetadataChangeSet(loaded=await fx.asset.load_metadata(), staged=staged)

    changed = await cs.persist(asset=fx.asset, snapshot=snapshot2)
    assert FILE_PATH in changed

    key_id = a.metadata_key_id
    rows = await Metadata.filter(
        asset_id=fx.asset.id, provider_id=fx.provider.id, metadata_key_id=key_id
    ).order_by("id")

    assert len(rows) == 4
    assert {r.value_text for r in rows} == {"/tmp/a", "/tmp/b"}

    removed_rows = [r for r in rows if r.removed]
    assert len(removed_rows) == 2

    current = MetadataChangeSet(await fx.asset.load_metadata()).current(fx.provider.id)
    assert FILE_PATH not in current or current[FILE_PATH] == []


@pytest.mark.asyncio
async def test_missing_key_in_staged_keeps_existing_value_unchanged(
    pipeline_db,
) -> None:
    fx = await PipelineFixture.create()

    existing_path = fx.metadata(FILE_PATH, "/tmp/a")
    await existing_path.save()
    await fx.asset.load_metadata()

    snapshot2 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    staged = [make_metadata(FILE_NAME, "doc.txt", provider_id=fx.provider.id)]
    cs = MetadataChangeSet(loaded=await fx.asset.load_metadata(), staged=staged)

    changed = await cs.persist(asset=fx.asset, snapshot=snapshot2)
    assert FILE_NAME in changed
    assert FILE_PATH not in changed

    current = MetadataChangeSet(await fx.asset.load_metadata()).current(fx.provider.id)
    assert current[FILE_PATH][0].value_text == "/tmp/a"


@pytest.mark.asyncio
async def test_persist_allows_remove_then_readd_same_value(pipeline_db) -> None:
    fx = await PipelineFixture.create()

    existing_path = fx.metadata(FILE_PATH, "/tmp/a")
    await existing_path.save()
    await fx.asset.load_metadata()

    # Remove it.
    snapshot2 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    cs2 = MetadataChangeSet(
        loaded=await fx.asset.load_metadata(),
        staged=[
            make_metadata(FILE_PATH, "/tmp/a", provider_id=fx.provider.id, removed=True)
        ],
    )
    changed2 = await cs2.persist(asset=fx.asset, snapshot=snapshot2)
    assert FILE_PATH in changed2

    # Re-add it (should not be deduped away).
    snapshot3 = await Snapshot.create(provider=fx.provider, status=OpStatus.IN_PROGRESS)
    cs3 = MetadataChangeSet(
        loaded=await fx.asset.load_metadata(),
        staged=[make_metadata(FILE_PATH, "/tmp/a", provider_id=fx.provider.id)],
    )
    changed3 = await cs3.persist(asset=fx.asset, snapshot=snapshot3)
    assert FILE_PATH in changed3

    key_id = existing_path.metadata_key_id
    rows = await Metadata.filter(
        asset_id=fx.asset.id, provider_id=fx.provider.id, metadata_key_id=key_id
    ).order_by("id")
    assert len(rows) == 3

    current = MetadataChangeSet(await fx.asset.load_metadata()).current(fx.provider.id)
    assert current[FILE_PATH][0].value_text == "/tmp/a"
