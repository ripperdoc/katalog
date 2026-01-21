from __future__ import annotations

from datetime import UTC, datetime

import pytest
from tortoise import Tortoise

from katalog.constants.metadata import FILE_NAME, TIME_MODIFIED
from katalog.models import (
    Asset,
    Metadata,
    MetadataChangeSet,
    OpStatus,
    Actor,
    ActorType,
    Changeset,
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


async def _seed() -> tuple[Actor, Changeset, Asset]:
    actor = await Actor.create(
        name="source",
        plugin_id="plugin.source",
        type=ActorType.SOURCE,
    )
    changeset = await Changeset.create(
        actor=actor,
        status=OpStatus.COMPLETED,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
    )
    asset = Asset(
        external_id="asset-1",
        canonical_uri="file:///asset-1",
    )
    await asset.save_record(changeset=changeset, actor=actor)
    return actor, changeset, asset


@pytest.mark.asyncio
async def test_changeset_persist_handles_removals_and_noops() -> None:
    await _init_db()
    try:
        actor, baseline_changeset, asset = await _seed()

        # Baseline metadata: name and modified time
        baseline_name = make_metadata(
            FILE_NAME,
            "doc.txt",
            actor_id=actor.id,
            asset=asset,
            changeset=baseline_changeset,
        )
        modified_dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        baseline_modified = make_metadata(
            TIME_MODIFIED,
            modified_dt,
            actor_id=actor.id,
            asset=asset,
            changeset=baseline_changeset,
        )
        await Metadata.bulk_create([baseline_name, baseline_modified])

        await asset.load_metadata()

        # New changeset with staged changes:
        # - same name (should be treated as no-op)
        # - modified time removed (marked removed=True)
        # - new name value (should add a new row)
        next_changeset = await Changeset.create(
            actor=actor, status=OpStatus.IN_PROGRESS
        )
        staged = [
            make_metadata(
                FILE_NAME,
                "doc.txt",
                actor_id=actor.id,
                asset=asset,
                changeset=next_changeset,
            ),
            make_metadata(
                FILE_NAME,
                "doc_v2.txt",
                actor_id=actor.id,
                asset=asset,
                changeset=next_changeset,
            ),
            make_metadata(
                TIME_MODIFIED,
                modified_dt,
                actor_id=actor.id,
                asset=asset,
                changeset=next_changeset,
                removed=True,
            ),
        ]

        cs = MetadataChangeSet(loaded=await asset.load_metadata(), staged=staged)
        changed = await cs.persist(asset=asset, changeset=next_changeset)

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
