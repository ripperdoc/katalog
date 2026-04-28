from __future__ import annotations

import os

import pytest

from katalog.constants.metadata import DOC_TEXT
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file


if os.getenv("KATALOG_RUN_KREUZBERG_EMBEDDING_IT") != "1":
    pytest.skip(
        "Set KATALOG_RUN_KREUZBERG_EMBEDDING_IT=1 to run Kreuzberg workflow integration smoke tests.",
        allow_module_level=True,
    )


@pytest.mark.asyncio
async def test_kreuzberg_workflow_smoke_extracts_text(db_session, tmp_path) -> None:
    _ = db_session

    doc_path = tmp_path / "smoke.txt"
    doc_path.write_text("Katalog smoke test content for Kreuzberg.", encoding="utf-8")

    spec = WorkflowSpec(
        file_name="smoke.workflow.toml",
        file_path="<integration-smoke>",
        workflow_id="kreuzberg-smoke",
        name="Kreuzberg smoke",
        description=None,
        version="1.0.0",
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
                name="Smoke kreuzberg extract",
                plugin_id="katalog.processors.kreuzberg_document_extract.KreuzbergDocumentExtractProcessor",
                identity_key="smoke-kreuzberg-extract",
                actor_type=ActorType.PROCESSOR,
                config={"enable_chunking": True},
                disabled=False,
            ),
        ],
    )

    result = await run_workflow_file(spec, sync_first=True)
    assert result.successful is True
    assert result.sources_run == 1
    assert result.processors_run == 1

    asset_repo = get_asset_repo()
    metadata_repo = get_metadata_repo()
    assets = await asset_repo.list(limit=10, offset=0)
    assert assets
    metadata = await metadata_repo.list_by_asset(int(assets[0].id))
    texts = [m.value for m in metadata if str(m.metadata_key) == str(DOC_TEXT)]
    assert any(isinstance(value, str) and value.strip() for value in texts)
