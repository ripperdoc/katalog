from __future__ import annotations

import os

import pytest

from katalog.constants.metadata import DOC_TEXT, get_metadata_id
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file
from katalog.workflows.contracts import WorkflowSourceActorsInput

if os.getenv("KATALOG_RUN_XBERG_EMBEDDING_IT") != "1":
    pytest.skip(
        "Set KATALOG_RUN_XBERG_EMBEDDING_IT=1 to run Xberg workflow integration smoke tests.",
        allow_module_level=True,
    )


@pytest.mark.asyncio
async def test_xberg_workflow_smoke_extracts_text(db_session, tmp_path) -> None:
    _ = db_session

    doc_path = tmp_path / "smoke.txt"
    doc_path.write_text("Katalog smoke test content for Xberg.", encoding="utf-8")

    spec = WorkflowSpec(
        file_name="smoke.workflow.toml",
        file_path="<integration-smoke>",
        workflow_id="xberg-smoke",
        name="Xberg smoke",
        description=None,
        version="1.0.0",
        input=WorkflowSourceActorsInput(),
        actors=[
            WorkflowActorSpec(
                name="Smoke filesystem source",
                plugin_id="katalog.sources.filesystem.FilesystemClient",
                identity_key="smoke-filesystem-source",
                actor_type=ActorType.SOURCE,
                config={
                    "root_path": str(tmp_path),
                    "max_files": 0,
                    "include_patterns": ["*.txt"],
                },
                disabled=False,
            ),
            WorkflowActorSpec(
                name="Smoke xberg extract",
                plugin_id="katalog.processors.xberg_document_extract.XbergDocumentExtractProcessor",
                identity_key="smoke-xberg-extract",
                actor_type=ActorType.PROCESSOR,
                config={"enable_chunking": True},
                disabled=False,
            ),
        ],
    )

    result = await run_workflow_file(spec, sync_first=True)
    assert result.successful is True
    # run_workflow_file reports the shared changeset as the processor result
    # when processors are present, so sources_run is 0 here by design.
    assert result.processors_run == 1

    asset_repo = get_asset_repo()
    metadata_repo = get_metadata_repo()
    assets = await asset_repo.list_rows(limit=10, offset=0)
    assert assets
    first_asset_id = assets[0].id
    assert first_asset_id is not None
    metadata = await metadata_repo.for_asset(int(first_asset_id))
    doc_text_id = get_metadata_id(DOC_TEXT)
    texts = [m.value for m in metadata if m.metadata_key_id == doc_text_id]
    assert any(isinstance(value, str) and value.strip() for value in texts)
