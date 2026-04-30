from __future__ import annotations

from typing import Any, Literal

import pytest

from katalog.api.collections import CollectionCreate, create_collection
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.models import ActorType, Changeset, OpStatus
from katalog.workflows.contracts import (
    WorkflowAllAssetsInput,
    WorkflowAssetIdsInput,
    WorkflowCollectionInput,
    WorkflowSourceActorsInput,
)
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file


def _workflow_spec(
    *,
    namespace: str,
    total_assets: int,
    missing_assets_policy: Literal["lost", "delete"] = "lost",
    always_process: bool = False,
    include_processor: bool = True,
) -> WorkflowSpec:
    actors = [
        WorkflowActorSpec(
            name="Fake source",
            plugin_id="katalog.sources.fake_assets.FakeAssetSource",
            identity_key=f"{namespace}-source",
            actor_type=ActorType.SOURCE,
            config={
                "namespace": namespace,
                "total_assets": total_assets,
                "seed": 1,
                "batch_delay_ms": 0,
                "batch_jitter_ms": 0,
                "include_collection": False,
            },
            disabled=False,
        )
    ]
    if include_processor:
        actors.append(
            WorkflowActorSpec(
                name="Name readability",
                plugin_id="katalog.processors.name_readability.NameReadabilityProcessor",
                identity_key=f"{namespace}-processor",
                actor_type=ActorType.PROCESSOR,
                config={},
                disabled=False,
            )
        )
    return WorkflowSpec(
        file_name=f"{namespace}.workflow.toml",
        file_path=f"<{namespace}>",
        workflow_id=f"workflow-{namespace}",
        name=f"Workflow {namespace}",
        description=None,
        version="1.0.0",
        input=WorkflowSourceActorsInput(),
        missing_assets_policy=missing_assets_policy,
        always_process=always_process,
        actors=actors,
    )


def _processor_only_workflow_spec(
    *,
    workflow_name: str,
    input_selector: (
        WorkflowAllAssetsInput
        | WorkflowAssetIdsInput
        | WorkflowCollectionInput
        | WorkflowSourceActorsInput
    ),
) -> WorkflowSpec:
    return WorkflowSpec(
        file_name=f"{workflow_name}.workflow.toml",
        file_path=f"<{workflow_name}>",
        workflow_id=f"workflow-{workflow_name}",
        name=f"Workflow {workflow_name}",
        description=None,
        version="1.0.0",
        input=input_selector,
        missing_assets_policy="lost",
        always_process=False,
        actors=[
            WorkflowActorSpec(
                name="Name readability",
                plugin_id="katalog.processors.name_readability.NameReadabilityProcessor",
                identity_key=f"{workflow_name}-processor",
                actor_type=ActorType.PROCESSOR,
                config={},
                disabled=False,
            )
        ],
    )


async def _run_and_load_changeset(
    spec: WorkflowSpec,
    *,
    always_process: bool | None = None,
) -> tuple[Any, Changeset]:
    result = await run_workflow_file(
        spec,
        sync_first=True,
        always_process=always_process,
    )
    changeset_id = (
        result.processor_changeset
        if result.processor_changeset is not None
        else result.last_changeset_id
    )
    assert changeset_id is not None
    changeset = await get_changeset_repo().get(id=int(changeset_id))
    return result, changeset


def _stats(changeset: Changeset) -> dict[str, int]:
    data = changeset.data or {}
    stats = data.get("stats")
    assert isinstance(stats, dict)
    return stats


@pytest.mark.asyncio
async def test_workflow_run_creates_completed_changeset_with_provenance(db_session) -> None:
    _ = db_session
    spec = _workflow_spec(namespace="wf-exec-basic", total_assets=3)

    result, changeset = await _run_and_load_changeset(spec)
    stats = _stats(changeset)
    workflow_meta = (changeset.data or {}).get("workflow")

    assert result.successful is True
    assert changeset.status == OpStatus.COMPLETED
    assert stats["assets_seen"] == 3
    assert stats["assets_saved"] == 3
    assert stats["processings_started"] == 3
    assert isinstance(workflow_meta, dict)
    assert workflow_meta["workflow_id"] == spec.workflow_id
    assert workflow_meta["file_name"] == spec.file_name
    assert workflow_meta["always_process"] is False


@pytest.mark.asyncio
async def test_default_skip_skips_second_run_when_inputs_unchanged(db_session) -> None:
    _ = db_session
    spec = _workflow_spec(namespace="wf-exec-skip", total_assets=4)

    _, first_changeset = await _run_and_load_changeset(spec)
    _, second_changeset = await _run_and_load_changeset(spec)
    first_stats = _stats(first_changeset)
    second_stats = _stats(second_changeset)

    assert first_stats["processings_started"] == 4
    assert second_stats["processings_started"] == 0
    assert second_stats["processings_skipped"] >= 4


@pytest.mark.asyncio
async def test_always_process_start_override_true_forces_execution(db_session) -> None:
    _ = db_session
    spec = _workflow_spec(namespace="wf-exec-override-true", total_assets=4)

    await _run_and_load_changeset(spec)
    _, changeset = await _run_and_load_changeset(spec, always_process=True)
    stats = _stats(changeset)
    workflow_meta = (changeset.data or {}).get("workflow")

    assert stats["processings_started"] == 4
    assert isinstance(workflow_meta, dict)
    assert workflow_meta["always_process"] is True


@pytest.mark.asyncio
async def test_always_process_start_override_false_disables_policy(db_session) -> None:
    _ = db_session
    spec = _workflow_spec(
        namespace="wf-exec-override-false",
        total_assets=4,
        always_process=True,
    )

    _, first_changeset = await _run_and_load_changeset(spec)
    _, second_changeset = await _run_and_load_changeset(spec, always_process=False)
    first_stats = _stats(first_changeset)
    second_stats = _stats(second_changeset)

    assert first_stats["processings_started"] == 4
    assert second_stats["processings_started"] == 0
    assert second_stats["processings_skipped"] >= 4


@pytest.mark.asyncio
async def test_missing_assets_policy_lost_marks_lost_without_deleting_assets(db_session) -> None:
    _ = db_session
    namespace = "wf-exec-lost"
    source_full = _workflow_spec(
        namespace=namespace,
        total_assets=3,
        missing_assets_policy="lost",
        include_processor=False,
    )
    source_partial = _workflow_spec(
        namespace=namespace,
        total_assets=2,
        missing_assets_policy="lost",
        include_processor=False,
    )

    await _run_and_load_changeset(source_full)
    _, second_changeset = await _run_and_load_changeset(source_partial)
    assets = await get_asset_repo().list_rows(namespace=namespace)
    stats = _stats(second_changeset)

    assert len(assets) == 3
    assert second_changeset.status == OpStatus.COMPLETED
    assert stats["assets_lost"] == 1


@pytest.mark.asyncio
async def test_missing_assets_policy_delete_removes_unseen_assets(db_session) -> None:
    _ = db_session
    namespace = "wf-exec-delete"
    source_full = _workflow_spec(
        namespace=namespace,
        total_assets=3,
        missing_assets_policy="delete",
        include_processor=False,
    )
    source_partial = _workflow_spec(
        namespace=namespace,
        total_assets=2,
        missing_assets_policy="delete",
        include_processor=False,
    )

    await _run_and_load_changeset(source_full)
    _, second_changeset = await _run_and_load_changeset(source_partial)
    assets = await get_asset_repo().list_rows(namespace=namespace)
    stats = _stats(second_changeset)

    assert len(assets) == 2
    assert second_changeset.status == OpStatus.COMPLETED
    assert stats["assets_lost"] == 1


@pytest.mark.asyncio
async def test_multi_batch_execution_processes_all_assets(db_session, monkeypatch) -> None:
    _ = db_session
    monkeypatch.setenv("KATALOG_BATCH_SIZE", "2")
    spec = _workflow_spec(namespace="wf-exec-multi-batch", total_assets=5)

    _, changeset = await _run_and_load_changeset(spec)
    assets = await get_asset_repo().list_rows(namespace="wf-exec-multi-batch")
    stats = _stats(changeset)

    assert len(assets) == 5
    assert stats["assets_seen"] == 5
    assert stats["assets_saved"] == 5
    assert stats["processings_started"] == 5


@pytest.mark.asyncio
async def test_input_all_assets_runs_processor_without_source_scan(db_session) -> None:
    _ = db_session
    source_spec = _workflow_spec(
        namespace="wf-input-all-seed",
        total_assets=4,
        include_processor=False,
    )
    await _run_and_load_changeset(source_spec)

    processor_spec = _processor_only_workflow_spec(
        workflow_name="wf-input-all",
        input_selector=WorkflowAllAssetsInput(),
    )
    _, changeset = await _run_and_load_changeset(processor_spec, always_process=True)
    stats = _stats(changeset)

    assert stats["processings_started"] >= 4
    assert stats["assets_seen"] >= 4


@pytest.mark.asyncio
async def test_input_asset_ids_limits_processing_to_selected_assets(db_session) -> None:
    _ = db_session
    namespace = "wf-input-asset-ids-seed"
    source_spec = _workflow_spec(
        namespace=namespace,
        total_assets=5,
        include_processor=False,
    )
    await _run_and_load_changeset(source_spec)
    assets = await get_asset_repo().list_rows(namespace=namespace, order_by="id")
    selected_ids = [int(assets[0].id), int(assets[1].id)]

    processor_spec = _processor_only_workflow_spec(
        workflow_name="wf-input-asset-ids",
        input_selector=WorkflowAssetIdsInput(asset_ids=selected_ids),
    )
    _, changeset = await _run_and_load_changeset(processor_spec, always_process=True)
    stats = _stats(changeset)

    assert stats["processings_started"] == len(selected_ids)
    assert stats["assets_seen"] == len(selected_ids)


@pytest.mark.asyncio
async def test_input_collection_limits_processing_to_collection_members(db_session) -> None:
    _ = db_session
    namespace = "wf-input-collection-seed"
    source_spec = _workflow_spec(
        namespace=namespace,
        total_assets=5,
        include_processor=False,
    )
    await _run_and_load_changeset(source_spec)
    assets = await get_asset_repo().list_rows(namespace=namespace, order_by="id")
    selected_ids = [int(assets[0].id), int(assets[1].id), int(assets[2].id)]

    collection = await create_collection(
        CollectionCreate(
            name="workflow-input-collection",
            asset_ids=selected_ids,
        )
    )
    assert collection.id is not None

    processor_spec = _processor_only_workflow_spec(
        workflow_name="wf-input-collection",
        input_selector=WorkflowCollectionInput(collection_id=int(collection.id)),
    )
    _, changeset = await _run_and_load_changeset(processor_spec, always_process=True)
    stats = _stats(changeset)

    assert stats["processings_started"] == len(selected_ids)
    assert stats["assets_seen"] == len(selected_ids)
