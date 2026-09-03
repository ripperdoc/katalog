from __future__ import annotations

import pytest

from katalog.constants.metadata import DOC_CHUNK_TEXT, INTERNAL_FTS_REINDEX
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.fts import get_fts_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType, Asset, MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import ProcessorResult
from katalog.processors.search_index import FullTextSearchIndexProcessor


async def _persist_chunks(
    *,
    asset: Asset,
    extraction_actor_id: int,
    fts_actor_id: int,
    chunks: list[str],
) -> None:
    assert asset.id is not None
    actor_repo = get_actor_repo()
    extraction_actor = await actor_repo.get_or_none(id=extraction_actor_id)
    fts_actor = await actor_repo.get_or_none(id=fts_actor_id)
    assert extraction_actor is not None
    assert fts_actor is not None

    changeset = await get_changeset_repo().begin(
        message="Replace chunks",
        actors=[extraction_actor, fts_actor],
        status=OpStatus.IN_PROGRESS,
    )
    existing = list(
        await get_metadata_repo().for_asset(int(asset.id), include_removed=True)
    )
    result = ProcessorResult(actor_id=extraction_actor_id)
    result.set_metadata_list(DOC_CHUNK_TEXT, chunks, replace=True)
    result.metadata.append(
        make_metadata(INTERNAL_FTS_REINDEX, 1, actor_id=fts_actor_id)
    )
    changes = MetadataChanges(asset=asset, loaded=existing, staged=result.metadata)

    await get_metadata_repo().persist_changes_batch(
        changeset,
        [changes],
        {int(asset.id): existing},
    )
    await changeset.finalize(status=OpStatus.COMPLETED)


@pytest.mark.asyncio
async def test_updated_chunks_replace_old_fts_entries(db_session) -> None:
    _ = db_session
    actor_repo = get_actor_repo()
    source_actor = await actor_repo.create(
        name="Source",
        plugin_id="katalog.sources.fake_assets.FakeAssetSource",
        identity_key="fts-replace-source",
        config={},
        type=ActorType.SOURCE,
    )
    extraction_actor = await actor_repo.create(
        name="Extraction",
        plugin_id="katalog.processors.xberg_document_extract.XbergDocumentExtractProcessor",
        identity_key="fts-replace-extraction",
        config={},
        type=ActorType.PROCESSOR,
    )
    fts_actor = await actor_repo.create(
        name="FTS",
        plugin_id=FullTextSearchIndexProcessor.plugin_id,
        identity_key="fts-replace-index",
        config={},
        type=ActorType.PROCESSOR,
    )
    assert source_actor.id is not None
    assert extraction_actor.id is not None
    assert fts_actor.id is not None

    changeset = await get_changeset_repo().begin(
        message="Create asset",
        actors=[source_actor],
        status=OpStatus.IN_PROGRESS,
    )
    asset = Asset(
        namespace="fts-replace",
        external_id="document",
        canonical_uri="https://example.com/document",
        actor_id=int(source_actor.id),
    )
    await get_asset_repo().save_record(
        asset,
        changeset=changeset,
        actor=source_actor,
    )
    await changeset.finalize(status=OpStatus.COMPLETED)

    await _persist_chunks(
        asset=asset,
        extraction_actor_id=int(extraction_actor.id),
        fts_actor_id=int(fts_actor.id),
        chunks=["obsolete alpha", "obsolete beta"],
    )
    await _persist_chunks(
        asset=asset,
        extraction_actor_id=int(extraction_actor.id),
        fts_actor_id=int(fts_actor.id),
        chunks=["current replacement"],
    )

    old_hits, old_total = await get_fts_repo().search(
        actor_id=int(fts_actor.id),
        query_text="obsolete",
        limit=10,
    )
    new_hits, new_total = await get_fts_repo().search(
        actor_id=int(fts_actor.id),
        query_text="replacement",
        limit=10,
    )

    assert old_hits == []
    assert old_total == 0
    assert [hit.source_text for hit in new_hits] == ["current replacement"]
    assert new_total == 1
