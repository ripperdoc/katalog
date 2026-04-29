from __future__ import annotations

from pathlib import Path

import pytest

from katalog.api.workflows import list_workflows
from katalog.config import current_workspace
from katalog.db.actors import get_actor_repo
from katalog.models import ActorType
from katalog.workflows import (
    WorkflowActorSpec,
    WorkflowSpec,
    discover_workflow_files,
    sync_workflow_file,
)
from katalog.workflows.specs import parse_workflow_file


def _workflow_spec(
    *,
    source_identity: str = "wf-source",
    source_namespace: str = "wf-test",
    processor_identity: str = "wf-processor",
) -> WorkflowSpec:
    return WorkflowSpec(
        file_name="lifecycle.workflow.toml",
        file_path="<lifecycle-spec>",
        workflow_id="workflow-lifecycle",
        name="Workflow lifecycle",
        description=None,
        version="1.0.0",
        actors=[
            WorkflowActorSpec(
                name="Fake source",
                plugin_id="katalog.sources.fake_assets.FakeAssetSource",
                identity_key=source_identity,
                actor_type=ActorType.SOURCE,
                config={
                    "namespace": source_namespace,
                    "total_assets": 0,
                    "seed": 1,
                    "batch_delay_ms": 0,
                    "batch_jitter_ms": 0,
                },
                disabled=False,
            ),
            WorkflowActorSpec(
                name="Mime processor",
                plugin_id="katalog.processors.mime_type.MimeTypeProcessor",
                identity_key=processor_identity,
                actor_type=ActorType.PROCESSOR,
                config={},
                disabled=False,
            ),
        ],
    )


def _write_minimal_workflow(
    path: Path,
    *,
    name: str,
    source_identity: str,
) -> None:
    path.write_text(
        "\n".join(
            [
                "[workflow]",
                f'name = "{name}"',
                "",
                "[policy]",
                "missing_assets_policy = \"lost\"",
                "always_process = false",
                "",
                "[[actors]]",
                'name = "Fake source"',
                'plugin_id = "katalog.sources.fake_assets.FakeAssetSource"',
                f'identity_key = "{source_identity}"',
                "[actors.config]",
                f'namespace = "{source_identity}"',
                "total_assets = 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.mark.asyncio
async def test_sync_creates_actors_when_workspace_has_none(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    before = await actor_db.list_rows(order_by="id")
    before_workflow = [a for a in before if str(a.identity_key or "").startswith("wf-")]
    assert before_workflow == []

    spec = _workflow_spec()
    synced = await sync_workflow_file(spec)
    after = await actor_db.list_rows(order_by="id")
    after_workflow = [a for a in after if str(a.identity_key or "").startswith("wf-")]

    assert len(synced) == 2
    assert len(after_workflow) == 2
    assert {a.identity_key for a in after_workflow} == {"wf-source", "wf-processor"}


@pytest.mark.asyncio
async def test_sync_is_idempotent_for_unchanged_workflow(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    spec = _workflow_spec()

    first_sync = await sync_workflow_file(spec)
    second_sync = await sync_workflow_file(spec)
    all_rows = await actor_db.list_rows(order_by="id")
    workflow_rows = [a for a in all_rows if str(a.identity_key or "").startswith("wf-")]

    assert len(workflow_rows) == 2
    assert {int(a.id) for a in first_sync if a.id is not None} == {
        int(a.id) for a in second_sync if a.id is not None
    }


@pytest.mark.asyncio
async def test_sync_updates_only_changed_actor_and_keeps_others(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()

    original = _workflow_spec(source_namespace="wf-before")
    await sync_workflow_file(original)
    before_rows = await actor_db.list_rows(order_by="id")
    by_identity_before = {str(a.identity_key): a for a in before_rows}

    updated = _workflow_spec(source_namespace="wf-after")
    await sync_workflow_file(updated)
    after_rows = await actor_db.list_rows(order_by="id")
    after_workflow_rows = [
        row for row in after_rows if str(row.identity_key or "").startswith("wf-")
    ]
    by_identity_after = {str(a.identity_key): a for a in after_rows}

    assert len(after_workflow_rows) == 2
    source_before = by_identity_before["wf-source"]
    source_after = by_identity_after["wf-source"]
    processor_before = by_identity_before["wf-processor"]
    processor_after = by_identity_after["wf-processor"]

    assert source_before.id == source_after.id
    assert source_before.config != source_after.config
    assert (source_after.config or {}).get("namespace") == "wf-after"

    assert processor_before.id == processor_after.id
    assert processor_before.config == processor_after.config
    assert processor_before.name == processor_after.name
    assert processor_before.disabled == processor_after.disabled


@pytest.mark.asyncio
async def test_sync_identity_key_change_adds_new_actor_and_preserves_old(db_session) -> None:
    _ = db_session
    actor_db = get_actor_repo()

    await sync_workflow_file(_workflow_spec(source_identity="wf-source-v1"))
    await sync_workflow_file(_workflow_spec(source_identity="wf-source-v2"))
    rows = await actor_db.list_rows(order_by="id")

    source_rows = [row for row in rows if row.type == ActorType.SOURCE]
    assert len(source_rows) == 2
    assert {row.identity_key for row in source_rows} == {"wf-source-v1", "wf-source-v2"}


@pytest.mark.asyncio
async def test_workflow_discovery_and_list_workflows(db_session) -> None:
    _ = db_session
    workspace = current_workspace()
    wf_a = workspace / "workflow.alpha.toml"
    wf_b = workspace / "beta.workflow.toml"
    _write_minimal_workflow(wf_a, name="alpha", source_identity="alpha-source")
    _write_minimal_workflow(wf_b, name="beta", source_identity="beta-source")

    discovered = discover_workflow_files(workspace)
    listed = await list_workflows()

    discovered_names = {path.name for path in discovered}
    listed_names = {entry["file_name"] for entry in listed}
    assert wf_a.name in discovered_names
    assert wf_b.name in discovered_names
    assert wf_a.name in listed_names
    assert wf_b.name in listed_names


@pytest.mark.asyncio
async def test_parse_workflow_file_rejects_invalid_definitions(db_session) -> None:
    _ = db_session
    workspace = current_workspace()

    unknown_plugin = workspace / "workflow.unknown-plugin.toml"
    unknown_plugin.write_text(
        "\n".join(
            [
                "[[actors]]",
                'name = "Unknown"',
                'plugin_id = "katalog.unknown.Plugin"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown plugin"):
        parse_workflow_file(unknown_plugin)

    bad_policy = workspace / "workflow.bad-policy.toml"
    bad_policy.write_text(
        "\n".join(
            [
                "[policy]",
                'missing_assets_policy = "invalid"',
                "",
                "[[actors]]",
                'name = "Fake source"',
                'plugin_id = "katalog.sources.fake_assets.FakeAssetSource"',
                "[actors.config]",
                'namespace = "bad-policy-source"',
                "total_assets = 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="missing_assets_policy"):
        parse_workflow_file(bad_policy)

    duplicate_identity = workspace / "workflow.duplicate-id.toml"
    duplicate_identity.write_text(
        "\n".join(
            [
                "[[actors]]",
                'name = "Fake source A"',
                'plugin_id = "katalog.sources.fake_assets.FakeAssetSource"',
                'identity_key = "dup-id"',
                "",
                "[[actors]]",
                'name = "Fake source B"',
                'plugin_id = "katalog.sources.fake_assets.FakeAssetSource"',
                'identity_key = "dup-id"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate actor identity_key"):
        parse_workflow_file(duplicate_identity)
