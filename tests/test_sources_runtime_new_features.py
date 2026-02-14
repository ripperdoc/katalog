from __future__ import annotations

import pytest

from katalog.constants.metadata import DATA_FILE_READER
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType, MetadataChanges, OpStatus
from katalog.plugins import registry as plugin_registry
from katalog.sources.runtime import run_sources


@pytest.mark.asyncio
async def test_run_sources_does_not_inject_data_file_reader_for_url_list(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    actor = await actor_db.create(
        name="url-list",
        plugin_id="katalog.sources.url_list.UrlListSource",
        type=ActorType.SOURCE,
        config={"urls": ["https://example.com/doc.pdf"]},
    )
    changeset = await changeset_db.begin(
        actors=[actor], message="url list scan", status=OpStatus.IN_PROGRESS
    )
    status = await run_sources(
        sources=[actor], changeset=changeset, run_processors=False
    )
    await changeset.finalize(status=status)

    assets = await asset_db.list_rows(order_by="id")
    assert len(assets) == 1
    metadata = await md_db.for_asset(assets[0], include_removed=True)
    keys = {str(m.key) for m in metadata}
    assert str(DATA_FILE_READER) not in keys


@pytest.mark.asyncio
async def test_run_sources_marks_assets_lost_when_followup_scan_is_empty(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    actor = await actor_db.create(
        name="fake-assets",
        plugin_id="katalog.sources.fake_assets.FakeAssetSource",
        type=ActorType.SOURCE,
        config={
            "total_assets": 3,
            "seed": 1,
            "batch_delay_ms": 0,
            "batch_jitter_ms": 0,
        },
    )
    first = await changeset_db.begin(
        actors=[actor], message="seed", status=OpStatus.IN_PROGRESS
    )
    first_status = await run_sources(
        sources=[actor], changeset=first, run_processors=False
    )
    await first.finalize(status=first_status)

    actor.config = {
        "total_assets": 0,
        "seed": 1,
        "batch_delay_ms": 0,
        "batch_jitter_ms": 0,
    }
    await actor_db.save(actor)
    plugin_registry._INSTANCE_CACHE.clear()

    second = await changeset_db.begin(
        actors=[actor], message="empty", status=OpStatus.IN_PROGRESS
    )
    second_status = await run_sources(
        sources=[actor], changeset=second, run_processors=False
    )
    await second.finalize(status=second_status)

    assert second.stats is not None
    assert second.stats.assets_lost > 0

    assets = await asset_db.list_rows(order_by="id")
    assert len(assets) > 0
    first_asset = assets[0]
    loaded = await md_db.for_asset(first_asset, include_removed=True)
    changes = MetadataChanges(asset=first_asset, loaded=loaded)
    current_keys = {str(key) for key in changes.current().keys()}
    # Lost flag should be part of current metadata view after empty scan.
    assert "asset/lost" in current_keys
