from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from katalog.constants.metadata import DATA_FILE_READER, DOC_CHUNK_TEXT, DOC_TEXT
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.db.vectors import get_vector_repo
from katalog.models import ActorType, MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import ProcessorResult
from katalog.processors.xberg_document_extract import XbergDocumentExtractProcessor
from katalog.sources.http_url import CrawlResponse, HttpUrlSource
from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file
from katalog.workflows.contracts import WorkflowSourceActorsInput


async def _fake_embed(text: str, **kwargs: Any) -> list[float]:
    _ = text, kwargs
    return [0.5] * 4


async def _fake_crawl_once(
    self: HttpUrlSource, url: str, *, method: str
) -> CrawlResponse:
    _ = self, method
    return CrawlResponse(
        loaded_url=url,
        headers={"content-type": "text/plain", "content-length": "12"},
        content=b"hello world",
    )


async def _fake_extract_batch(
    self: XbergDocumentExtractProcessor,
    changes_batch: list[MetadataChanges],
) -> list[ProcessorResult]:
    results: list[ProcessorResult] = []
    for changes in changes_batch:
        if DATA_FILE_READER not in changes.current():
            results.append(ProcessorResult(status=OpStatus.SKIPPED))
            continue
        results.append(
            ProcessorResult(
                metadata=[make_metadata(DOC_TEXT, "hello world", self.actor.id)]
            )
        )
    return results


def _workflow_spec(
    json_path: Path,
    *,
    vector_identity: str,
    vector_metadata_keys: list[str] | None = None,
) -> WorkflowSpec:
    namespace = "wf-vector-first-run"
    return WorkflowSpec(
        file_name=f"{namespace}.workflow.toml",
        file_path=f"<{namespace}>",
        workflow_id=f"workflow-{namespace}",
        name="First-run vector workflow",
        description=None,
        version="1.0.0",
        input=WorkflowSourceActorsInput(),
        actors=[
            WorkflowActorSpec(
                name="JSON documents",
                plugin_id="katalog.sources.json_list.JsonListSource",
                identity_key=f"{namespace}-json",
                actor_type=ActorType.SOURCE,
                config={
                    "json_file": str(json_path),
                    "records_field": "documents",
                    "records_are_map": True,
                    "url_field": "uri",
                    "emit_record_json": False,
                },
                disabled=False,
            ),
            WorkflowActorSpec(
                name="HTTP recursive metadata",
                plugin_id="katalog.sources.http_url.HttpUrlSource",
                identity_key=f"{namespace}-http",
                actor_type=ActorType.SOURCE,
                config={},
                disabled=False,
            ),
            WorkflowActorSpec(
                name="Document extraction",
                plugin_id=(
                    "katalog.processors.xberg_document_extract.XbergDocumentExtractProcessor"
                ),
                identity_key=f"{namespace}-extract",
                actor_type=ActorType.PROCESSOR,
                config={},
                disabled=False,
            ),
            WorkflowActorSpec(
                name="Vector index",
                plugin_id=(
                    "katalog.processors.vector_index.XbergVectorIndexProcessor"
                ),
                identity_key=vector_identity,
                actor_type=ActorType.PROCESSOR,
                config={
                    "dimension": 4,
                    "metadata_keys": vector_metadata_keys or ["document/text"],
                    "min_text_length": 1,
                },
                disabled=False,
            ),
        ],
    )


@pytest.mark.asyncio
async def test_vector_index_is_created_on_first_workflow_run(
    db_session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _ = db_session
    monkeypatch.setattr(
        "katalog.processors.vector_index.embed_text_xberg", _fake_embed
    )
    monkeypatch.setattr("katalog.db.sqlspec.metadata.embed_text_xberg", _fake_embed)
    monkeypatch.setattr(HttpUrlSource, "_crawl_once", _fake_crawl_once)
    monkeypatch.setattr(
        XbergDocumentExtractProcessor,
        "run_batch",
        _fake_extract_batch,
    )

    json_path = tmp_path / "documents.json"
    json_path.write_text(
        '{"documents":{"doc-1":{"uri":"https://example.com/document"}}}',
        encoding="utf-8",
    )
    vector_identity = "wf-vector-first-run-vector"

    result = await run_workflow_file(
        _workflow_spec(json_path, vector_identity=vector_identity),
        sync_first=True,
    )
    vector_actor = await get_actor_repo().get_or_none(identity_key=vector_identity)

    assert result.successful is True
    assert vector_actor is not None
    assert vector_actor.id is not None
    assert await get_vector_repo().has_index_records(
        actor_id=int(vector_actor.id),
        dim=4,
    )


@pytest.mark.asyncio
async def test_updated_chunks_replace_old_metadata_and_vectors(
    db_session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _ = db_session
    chunks = ["old chunk one", "old chunk two"]

    async def _fake_extract_chunks(
        self: XbergDocumentExtractProcessor,
        changes_batch: list[MetadataChanges],
    ) -> list[ProcessorResult]:
        results: list[ProcessorResult] = []
        for changes in changes_batch:
            if DATA_FILE_READER not in changes.current():
                results.append(ProcessorResult(status=OpStatus.SKIPPED))
                continue
            result = ProcessorResult(actor_id=self.actor.id)
            result.set_metadata_list(DOC_CHUNK_TEXT, chunks, replace=True)
            results.append(result)
        return results

    monkeypatch.setattr(
        "katalog.processors.vector_index.embed_text_xberg", _fake_embed
    )
    monkeypatch.setattr("katalog.db.sqlspec.metadata.embed_text_xberg", _fake_embed)
    monkeypatch.setattr(HttpUrlSource, "_crawl_once", _fake_crawl_once)
    monkeypatch.setattr(
        XbergDocumentExtractProcessor,
        "run_batch",
        _fake_extract_chunks,
    )

    json_path = tmp_path / "documents.json"
    json_path.write_text(
        '{"documents":{"doc-1":{"uri":"https://example.com/document"}}}',
        encoding="utf-8",
    )
    vector_identity = "wf-vector-replace-chunks-vector"
    spec = _workflow_spec(
        json_path,
        vector_identity=vector_identity,
        vector_metadata_keys=[str(DOC_CHUNK_TEXT)],
    )

    first_result = await run_workflow_file(spec, sync_first=True)
    chunks[:] = ["new replacement chunk"]
    second_result = await run_workflow_file(
        spec,
        sync_first=True,
        always_process=True,
    )

    vector_actor = await get_actor_repo().get_or_none(identity_key=vector_identity)
    assets = await get_asset_repo().list_rows(namespace="web")
    assert first_result.successful is True
    assert second_result.successful is True
    assert vector_actor is not None and vector_actor.id is not None
    assert len(assets) == 1
    asset = assets[0]
    assert asset.id is not None

    metadata = await get_metadata_repo().for_asset(
        int(asset.id),
        include_removed=True,
    )
    current_chunks = MetadataChanges(
        asset=asset,
        loaded=metadata,
        staged=[],
    ).values_for_key(DOC_CHUNK_TEXT)
    hits = await get_vector_repo().search(
        actor_id=int(vector_actor.id),
        dim=4,
        query_vector=[0.5] * 4,
        limit=10,
    )

    assert current_chunks == ["new replacement chunk"]
    assert [hit.source_text for hit in hits] == ["new replacement chunk"]
