from __future__ import annotations

import pytest

from katalog.constants.metadata import (
    DOC_TEXT,
    INTERNAL_VECTOR_REINDEX,
    get_metadata_id,
)
from katalog.models import Actor, ActorType, Asset, MetadataChanges, make_metadata
from katalog.processors.vector_index import XbergVectorIndexProcessor


def _make_actor() -> Actor:
    return Actor(
        id=55,
        name="vector-index",
        plugin_id="katalog.processors.vector_index.XbergVectorIndexProcessor",
        type=ActorType.PROCESSOR,
    )


def _make_changes(actor: Actor) -> MetadataChanges:
    asset = Asset(
        id=1,
        namespace="web",
        external_id="x",
        canonical_uri="https://example.com/x",
        actor_id=1,
    )
    persisted = make_metadata(DOC_TEXT, "hello world", actor_id=actor.id)
    persisted.id = 101
    persisted.asset_id = asset.id
    return MetadataChanges(
        asset=asset,
        loaded=[persisted],
        staged=[],
    )


@pytest.mark.asyncio
async def test_vector_index_processor_queues_reindex(db_session) -> None:
    _ = db_session
    actor = _make_actor()
    processor = XbergVectorIndexProcessor(
        actor=actor,
        metadata_keys=[str(DOC_TEXT)],
        min_text_length=1,
        max_points=10,
        dimension=16,
    )
    result = await processor.run(_make_changes(actor))

    reindex = [
        m
        for m in result.metadata
        if m.metadata_key_id == get_metadata_id(INTERNAL_VECTOR_REINDEX)
    ]
    assert len(reindex) == 1
    assert reindex[0].actor_id == actor.id


@pytest.mark.asyncio
async def test_to_vector_points_embeds_string_metadata(db_session, monkeypatch) -> None:
    _ = db_session
    from katalog.db.sqlspec.metadata import _to_vector_points

    async def _fake_embed(text: str, **kwargs) -> list[float]:
        return [0.5] * 16

    monkeypatch.setattr(
        "katalog.db.sqlspec.metadata.embed_text_xberg", _fake_embed
    )

    entry = make_metadata(DOC_TEXT, "hello world", actor_id=55, asset_id=1)
    entry.id = 101
    points = await _to_vector_points(
        [entry],
        dependencies=frozenset({DOC_TEXT}),
        model="fast",
        backend="preset",
        normalize=True,
        batch_size=32,
        dim=16,
        min_text_length=1,
        max_points=10,
    )
    assert len(points) == 1
    assert points[0].metadata_id == 101
    assert len(points[0].vector) == 16
