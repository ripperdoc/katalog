from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from katalog.constants.metadata import DOC_CHUNK_TEXT, FILE_NAME, get_metadata_id
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.fts import FtsPoint, get_fts_repo
from katalog.db.metadata import get_metadata_repo
from katalog.db.vectors import VectorPoint, get_vector_repo
from katalog.lifespan import app_lifespan
from katalog.models import (
    ActorType,
    Asset,
    MetadataChanges,
    OpStatus,
    make_metadata,
)
from katalog.models.query import AssetQuery
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file
from katalog.workflows.contracts import WorkflowSourceActorsInput


def _workflow_spec(
    *,
    total_assets: int,
    missing_assets_policy: Literal["lost", "delete"],
) -> WorkflowSpec:
    return WorkflowSpec(
        file_name=f"persist-{missing_assets_policy}.workflow.toml",
        file_path=f"<persist-{missing_assets_policy}>",
        workflow_id=f"persist-{missing_assets_policy}",
        name=f"Persist {missing_assets_policy}",
        description=None,
        version="1.0.0",
        input=WorkflowSourceActorsInput(),
        missing_assets_policy=missing_assets_policy,
        always_process=False,
        actors=[
            WorkflowActorSpec(
                name="Fake source",
                plugin_id="katalog.sources.fake_assets.FakeAssetSource",
                identity_key=f"persist-{missing_assets_policy}-source",
                actor_type=ActorType.SOURCE,
                config={
                    "namespace": f"persist-{missing_assets_policy}",
                    "total_assets": total_assets,
                    "seed": 1,
                    "batch_delay_ms": 0,
                    "batch_jitter_ms": 0,
                    "include_collection": False,
                },
                disabled=False,
            )
        ],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_assets_policy", ["lost", "delete"])
async def test_missing_asset_reconciliation_persists_across_lifespans(
    tmp_path: Path,
    missing_assets_policy: Literal["lost", "delete"],
) -> None:
    workspace = tmp_path / f"workspace-{missing_assets_policy}"
    namespace = f"persist-{missing_assets_policy}"

    async with app_lifespan(workspace=workspace):
        await run_workflow_file(
            _workflow_spec(
                total_assets=1,
                missing_assets_policy=missing_assets_policy,
            ),
            sync_first=True,
        )

    async with app_lifespan(workspace=workspace):
        result = await run_workflow_file(
            _workflow_spec(
                total_assets=0,
                missing_assets_policy=missing_assets_policy,
            ),
            sync_first=True,
        )
        changeset = await get_changeset_repo().get(id=int(result.last_changeset_id))
        assert changeset.stats is not None
        assert changeset.stats.assets_lost == 1

    async with app_lifespan(workspace=workspace):
        assets = await get_asset_repo().list_rows(namespace=namespace)
        visible = await get_asset_repo().count_assets_for_query(query=AssetQuery())

    assert len(assets) == (1 if missing_assets_policy == "lost" else 0)
    assert visible == 0


@pytest.mark.asyncio
async def test_delete_unseen_assets_removes_persisted_search_index_rows(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace-indexes"
    fts_actor_id: int
    vector_actor_id: int
    asset_id: int

    async with app_lifespan(workspace=workspace):
        actor_repo = get_actor_repo()
        source_actor = await actor_repo.create(
            name="Source",
            plugin_id="katalog.sources.fake_assets.FakeAssetSource",
            identity_key="persist-index-source",
            config={},
            type=ActorType.SOURCE,
        )
        extraction_actor = await actor_repo.create(
            name="Extraction",
            plugin_id="katalog.processors.xberg_document_extract.XbergDocumentExtractProcessor",
            identity_key="persist-index-extraction",
            config={},
            type=ActorType.PROCESSOR,
        )
        fts_actor = await actor_repo.create(
            name="FTS",
            plugin_id="katalog.processors.search_index.FullTextSearchIndexProcessor",
            identity_key="persist-index-fts",
            config={},
            type=ActorType.PROCESSOR,
        )
        vector_actor = await actor_repo.create(
            name="Vector",
            plugin_id="katalog.processors.vector_index.XbergVectorIndexProcessor",
            identity_key="persist-index-vector",
            config={},
            type=ActorType.PROCESSOR,
        )
        assert source_actor.id is not None
        assert extraction_actor.id is not None
        assert fts_actor.id is not None
        assert vector_actor.id is not None
        fts_actor_id = int(fts_actor.id)
        vector_actor_id = int(vector_actor.id)

        changeset = await get_changeset_repo().begin(
            actors=[source_actor, extraction_actor, fts_actor, vector_actor],
            message="Create indexed asset",
            status=OpStatus.IN_PROGRESS,
        )
        asset = Asset(
            namespace="persist-index",
            external_id="document",
            canonical_uri="https://example.com/document",
            actor_id=int(source_actor.id),
        )
        await get_asset_repo().save_record(
            asset,
            changeset=changeset,
            actor=source_actor,
        )
        assert asset.id is not None
        asset_id = int(asset.id)
        changes = MetadataChanges(
            asset=asset,
            loaded=[],
            staged=[
                make_metadata(FILE_NAME, "document.txt", actor_id=int(source_actor.id)),
                make_metadata(
                    DOC_CHUNK_TEXT,
                    "indexed chunk",
                    actor_id=int(extraction_actor.id),
                ),
            ],
        )
        await get_metadata_repo().persist_changes(changes, changeset=changeset)
        await changeset.finalize(status=OpStatus.COMPLETED)

        metadata = await get_metadata_repo().for_asset(asset_id)
        chunk_key_id = get_metadata_id(DOC_CHUNK_TEXT)
        chunk = next(
            entry for entry in metadata if entry.metadata_key_id == chunk_key_id
        )
        assert chunk.id is not None
        assert chunk.metadata_key_id is not None
        await get_fts_repo().upsert_asset_points(
            asset_id=asset_id,
            actor_id=fts_actor_id,
            metadata_key_ids=[int(chunk.metadata_key_id)],
            points=[FtsPoint(metadata_id=int(chunk.id), text="indexed chunk")],
        )
        await get_vector_repo().upsert_asset_points(
            asset_id=asset_id,
            actor_id=vector_actor_id,
            dim=2,
            metadata_key_ids=[int(chunk.metadata_key_id)],
            points=[VectorPoint(metadata_id=int(chunk.id), vector=[0.5, 0.5])],
        )
        assert await get_fts_repo().has_index_records(actor_id=fts_actor_id)
        assert await get_vector_repo().has_index_records(
            actor_id=vector_actor_id,
            dim=2,
        )

        deleted = await get_asset_repo().delete_unseen_assets(
            actor_ids=[int(source_actor.id)],
            seen_asset_ids=[],
        )
        assert deleted == 1

    async with app_lifespan(workspace=workspace):
        assert await get_asset_repo().list_rows(namespace="persist-index") == []
        assert await get_metadata_repo().for_asset(asset_id, include_removed=True) == []
        assert not await get_fts_repo().has_index_records(actor_id=fts_actor_id)
        assert not await get_vector_repo().has_index_records(
            actor_id=vector_actor_id,
            dim=2,
        )
