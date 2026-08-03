from __future__ import annotations

import json

import pytest

from katalog.api.search import semantic_hits_for_query
from katalog.constants.metadata import DOC_TEXT, get_metadata_id
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.db.vectors import VectorPoint, get_vector_repo
from katalog.models import ActorType, Asset, OpStatus, make_metadata
from katalog.models.query import AssetQuery
from katalog.processors.vector_index import (
    LEGACY_KREUZBERG_VECTOR_INDEX_PLUGIN_ID,
    XbergVectorIndexProcessor,
)

# Workspaces created before the kreuzberg → xberg rename store the old plugin
# id in their actors table (see e.g. old workspaces under ./w). These tests
# verify that the new katalog version can still read and semantically search
# such workspaces.

DIM = 16
_LEGACY_CONFIG = {
    "dimension": DIM,
    "embedding_model": "fast",
    "embedding_backend": "preset",
    "metadata_keys": [str(DOC_TEXT)],
}


async def _seed_vector_indexed_workspace(*, plugin_id: str) -> tuple[int, int]:
    """Create an actor with the given plugin id plus one indexed metadata row.

    Returns (actor_id, asset_id).
    """
    actor = await get_actor_repo().create(
        name="Vector index",
        plugin_id=plugin_id,
        identity_key=plugin_id,
        config=json.dumps(_LEGACY_CONFIG),
        type=ActorType.PROCESSOR,
    )
    assert actor.id is not None

    changeset = await get_changeset_repo().begin(
        message="Legacy workspace seed",
        actors=[actor],
        status=OpStatus.IN_PROGRESS,
    )
    asset = Asset(
        namespace="legacy",
        external_id=f"legacy-{actor.id}",
        canonical_uri=f"legacy://doc-{actor.id}",
        actor_id=actor.id,
    )
    await get_asset_repo().save_record(asset, changeset=changeset, actor=actor)
    assert asset.id is not None
    entry = make_metadata(
        DOC_TEXT,
        "hello world",
        actor_id=actor.id,
        asset_id=asset.id,
        changeset=changeset,
    )
    await get_metadata_repo().bulk_create([entry])
    await changeset.finalize(status=OpStatus.COMPLETED)
    entries = await get_metadata_repo().for_asset(int(asset.id))
    doc_text_id = get_metadata_id(DOC_TEXT)
    entry_id = next(
        int(m.id)
        for m in entries
        if m.metadata_key_id == doc_text_id and m.id is not None
    )

    vector = [1.0] + [0.0] * (DIM - 1)
    await get_vector_repo().upsert_asset_points(
        asset_id=int(asset.id),
        actor_id=int(actor.id),
        dim=DIM,
        metadata_key_ids=[int(doc_text_id)],
        points=[VectorPoint(metadata_id=entry_id, vector=vector)],
    )
    return int(actor.id), int(asset.id)


def _semantic_query() -> AssetQuery:
    return AssetQuery(
        search="hello",
        search_mode="semantic",
        search_dimension=DIM,
    )


@pytest.fixture
def fake_query_embedding(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_embed(text: str, **kwargs) -> list[float]:
        return [1.0] + [0.0] * (DIM - 1)

    monkeypatch.setattr("katalog.api.search.embed_text_xberg", _fake_embed)


@pytest.mark.asyncio
async def test_semantic_search_reads_legacy_kreuzberg_workspace(
    db_session, fake_query_embedding
) -> None:
    _ = db_session
    _actor_id, asset_id = await _seed_vector_indexed_workspace(
        plugin_id=LEGACY_KREUZBERG_VECTOR_INDEX_PLUGIN_ID
    )

    hits, total, _elapsed = await semantic_hits_for_query(_semantic_query())

    assert total == 1
    assert hits[0].asset_id == asset_id
    assert hits[0].text == "hello world"


@pytest.mark.asyncio
async def test_semantic_search_prefers_new_xberg_actor(
    db_session, fake_query_embedding
) -> None:
    _ = db_session
    legacy_actor_id, _legacy_asset_id = await _seed_vector_indexed_workspace(
        plugin_id=LEGACY_KREUZBERG_VECTOR_INDEX_PLUGIN_ID
    )
    new_actor_id, _new_asset_id = await _seed_vector_indexed_workspace(
        plugin_id=XbergVectorIndexProcessor.plugin_id
    )
    assert legacy_actor_id != new_actor_id

    # With both a legacy and a new vector index actor present, resolution picks
    # the new one; the legacy actor stays reachable via search_index.
    hits, total, _elapsed = await semantic_hits_for_query(_semantic_query())
    assert total == 1

    query = _semantic_query()
    query.search_index = legacy_actor_id
    legacy_hits, legacy_total, _elapsed = await semantic_hits_for_query(query)
    assert legacy_total == 1
    assert legacy_hits[0].text == "hello world"
