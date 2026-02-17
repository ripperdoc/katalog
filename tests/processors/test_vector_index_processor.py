from __future__ import annotations

import pytest

from katalog.constants.metadata import DOC_TEXT
from katalog.models import Actor, ActorType, Asset, MetadataChanges, make_metadata
from katalog.processors.vector_index import KreuzbergVectorIndexProcessor


@pytest.mark.asyncio
async def test_vector_index_processor_collects_string_points(monkeypatch) -> None:
    actor = Actor(
        id=55,
        name="vector-index",
        plugin_id="katalog.processors.vector_index.KreuzbergVectorIndexProcessor",
        type=ActorType.PROCESSOR,
    )
    processor = KreuzbergVectorIndexProcessor(
        actor=actor,
        metadata_keys=[str(DOC_TEXT)],
        min_text_length=1,
        max_points=10,
        dimension=16,
    )
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
    changes = MetadataChanges(
        asset=asset,
        loaded=[persisted],
        staged=[],
    )

    async def _fake_embed(text: str, **kwargs) -> list[float]:
        return [0.5] * 16

    monkeypatch.setattr(
        "katalog.processors.vector_index.embed_text_kreuzberg", _fake_embed
    )
    points = await processor._collect_points(changes)
    assert len(points) == 1
    assert points[0].metadata_id == 101
    assert len(points[0].vector) == 16
