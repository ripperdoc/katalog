from __future__ import annotations

from datetime import UTC, datetime

import pytest
from tortoise import Tortoise

from katalog.metadata import FILE_NAME, TIME_MODIFIED
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    make_metadata,
)
from katalog.queries import sync_metadata_registry


async def _init_db() -> None:
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["katalog.models"]},
        use_tz=True,
        timezone="UTC",
    )
    await Tortoise.generate_schemas()
    await sync_metadata_registry()


async def _teardown_db() -> None:
    await Tortoise.close_connections()


async def _seed() -> tuple[Provider, Snapshot, Asset]:
    provider = await Provider.create(
        name="source",
        plugin_id="plugin.source",
        type=ProviderType.SOURCE,
    )
    snapshot = await Snapshot.create(
        provider=provider,
        status=OpStatus.COMPLETED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    asset = await Asset.create(
        provider=provider,
        canonical_id="asset-1",
        canonical_uri="file:///asset-1",
        created_snapshot=snapshot,
        last_snapshot=snapshot,
    )
    return provider, snapshot, asset


@pytest.mark.asyncio
async def test_changeset_persist_handles_removals_and_noops() -> None:
    await _init_db()
    try:
        provider, baseline_snapshot, asset = await _seed()

        # Baseline metadata: name and modified time
        baseline_name = make_metadata(
            FILE_NAME,
            "doc.txt",
            provider_id=provider.id,
            asset=asset,
            snapshot=baseline_snapshot,
        )
        baseline_modified = make_metadata(
            TIME_MODIFIED,
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            provider_id=provider.id,
            asset=asset,
            snapshot=baseline_snapshot,
        )
        await Metadata.bulk_create([baseline_name, baseline_modified])

        await asset.load_metadata()

        # New snapshot with staged changes:
        # - same name (should be treated as no-op)
        # - modified time removed (marked removed=True)
        # - new name value (should add a new row)
        next_snapshot = await Snapshot.create(
            provider=provider, status=OpStatus.IN_PROGRESS
        )
        staged = [
            make_metadata(
                FILE_NAME,
                "doc.txt",
                provider_id=provider.id,
                asset=asset,
                snapshot=next_snapshot,
            ),
            make_metadata(
                FILE_NAME,
                "doc_v2.txt",
                provider_id=provider.id,
                asset=asset,
                snapshot=next_snapshot,
            ),
            make_metadata(
                TIME_MODIFIED,
                None,
                provider_id=provider.id,
                asset=asset,
                snapshot=next_snapshot,
                removed=True,
            ),
        ]

        cs = MetadataChangeSet(loaded=await asset.load_metadata(), staged=staged)
        changed = await cs.persist(asset=asset, snapshot=next_snapshot)

        # Should record changes for FILE_NAME and TIME_MODIFIED
        assert FILE_NAME in changed
        assert TIME_MODIFIED in changed

        # Re-load metadata and verify:
        rows = await Metadata.filter(asset=asset).order_by("metadata_key_id", "id")
        values_by_key = {}
        for md in rows:
            values_by_key.setdefault(md.key, []).append(md)

        # Name has both old and new values; removed flag stays False
        names = values_by_key.get(FILE_NAME, [])
        assert len(names) == 2
        assert {md.value for md in names} == {"doc.txt", "doc_v2.txt"}
        assert all(not md.removed for md in names)

        # TIME_MODIFIED should include the baseline value and a removal marker
        times = values_by_key.get(TIME_MODIFIED, [])
        assert len(times) == 2
        assert any(md.removed for md in times)
        assert any(not md.removed for md in times)
    finally:
        await _teardown_db()
