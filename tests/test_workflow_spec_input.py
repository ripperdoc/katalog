from __future__ import annotations

import pytest

from katalog.models import ActorType
from katalog.workflows import (
    WorkflowActorSpec,
    WorkflowSpec,
    run_workflow_file,
    workflow_status,
)


@pytest.mark.asyncio
async def test_run_workflow_file_accepts_workflow_spec_object(db_session) -> None:
    _ = db_session

    spec = WorkflowSpec(
        file_name="in-memory.workflow.toml",
        file_path="<in-memory>",
        workflow_id="test-memory-workflow",
        name="In-memory workflow",
        description=None,
        version="1.0.0",
        actors=[
            WorkflowActorSpec(
                name="Fake source",
                plugin_id="katalog.sources.fake_assets.FakeAssetSource",
                actor_type=ActorType.SOURCE,
                config={
                    "namespace": "fake",
                    "total_assets": 0,
                    "seed": 1,
                    "batch_delay_ms": 0,
                    "batch_jitter_ms": 0,
                },
                disabled=False,
            )
        ],
    )

    result = await run_workflow_file(spec, sync_first=True)
    status = await workflow_status(spec)

    assert result.workflow_file == "<in-memory>"
    assert result.sources_run == 1
    assert result.successful is True
    assert status["status"] == "ready"
    assert status["resolved_actor_count"] == 1
