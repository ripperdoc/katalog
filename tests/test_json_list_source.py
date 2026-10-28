from __future__ import annotations

import json

import pytest

from katalog.constants.metadata import (
    DATA_FILE_READER,
    FILE_TYPE,
    FILE_URI,
    SOURCE_JSON_RECORD,
)
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType, MetadataChanges, OpStatus
from katalog.sources.google_storage import GoogleStorageSource
from katalog.sources.http_url import CrawlResponse, HttpUrlSource
from katalog.sources.runtime import run_sources


@pytest.mark.asyncio
async def test_json_list_source_recurses_to_http_source_with_minimal_stub(
    db_session, tmp_path, monkeypatch
) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    json_file = tmp_path / "documents.json"
    json_file.write_text(
        json.dumps(
            [
                {"url": "https://example.com/doc.pdf", "title": "Doc"},
                {"url": "https://example.com/doc.pdf", "title": "Duplicate"},
            ]
        ),
        encoding="utf-8",
    )

    json_actor = await actor_db.create(
        name="json-list",
        plugin_id="katalog.sources.json_list.JsonListSource",
        type=ActorType.SOURCE,
        config={"json_file": str(json_file)},
    )
    _ = await actor_db.create(
        name="http-url",
        plugin_id="katalog.sources.http_url.HttpUrlSource",
        type=ActorType.SOURCE,
        config={"timeout_seconds": 1.0},
    )

    crawl_calls: list[tuple[str, str]] = []

    async def _fake_crawl_once(self, url: str, *, method: str):  # noqa: ANN001
        _ = self
        crawl_calls.append((url, method))
        return CrawlResponse(
            loaded_url=url,
            headers={
                "content-type": "application/pdf",
                "content-length": "42",
            },
            content=b"",
        )

    monkeypatch.setattr(HttpUrlSource, "_crawl_once", _fake_crawl_once)

    changeset = await changeset_db.begin(
        actors=[json_actor], message="json list scan", status=OpStatus.IN_PROGRESS
    )
    status = await run_sources(
        sources=[json_actor], changeset=changeset, run_processors=False
    )
    await changeset.finalize(status=status)

    assert status == OpStatus.COMPLETED
    # Duplicate URL records should be recursively scanned only once.
    assert crawl_calls == [("https://example.com/doc.pdf", "HEAD")]

    assets = await asset_db.list_rows(order_by="id")
    assert len(assets) == 1

    metadata = await md_db.for_asset(assets[0], include_removed=True)
    current = MetadataChanges(asset=assets[0], loaded=metadata).current()
    current_keys = {str(key) for key in current.keys()}
    assert str(SOURCE_JSON_RECORD) not in current_keys
    assert str(FILE_URI) in current_keys
    assert str(DATA_FILE_READER) in current_keys
    assert str(FILE_TYPE) in current_keys


@pytest.mark.asyncio
async def test_json_list_source_recurses_gs_urls_to_seeded_google_storage_source(
    db_session, tmp_path, monkeypatch
) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    json_file = tmp_path / "documents.json"
    json_file.write_text(
        json.dumps(
            {
                "docs": {
                    "doc-key": {"uri": "gs://bucket/scenarios/s1/docs/a.pdf"},
                    "other-bucket": {"uri": "gs://elsewhere/a.pdf"},
                    "web-key": {"uri": "https://example.com/b.pdf"},
                }
            }
        ),
        encoding="utf-8",
    )

    json_actor = await actor_db.create(
        name="json-list",
        plugin_id="katalog.sources.json_list.JsonListSource",
        type=ActorType.SOURCE,
        config={
            "json_file": str(json_file),
            "records_field": "docs",
            "records_are_map": True,
            "url_field": "uri",
        },
    )
    _ = await actor_db.create(
        name="gcs",
        plugin_id="katalog.sources.google_storage.GoogleStorageSource",
        type=ActorType.SOURCE,
        config={"gcs_url": "gs://bucket", "seeded_only": True},
    )
    _ = await actor_db.create(
        name="http-url",
        plugin_id="katalog.sources.http_url.HttpUrlSource",
        type=ActorType.SOURCE,
        config={"timeout_seconds": 1.0},
    )

    class _FakeBlob:
        size = 7
        content_type = "application/pdf"
        updated = None
        md5_hash = None

    looked_up: list[str] = []

    def _fake_blob_metadata(self, object_name: str):  # noqa: ANN001
        _ = self
        looked_up.append(object_name)
        return _FakeBlob()

    async def _ready(self):  # noqa: ANN001
        _ = self
        return True, None

    async def _fake_crawl_once(self, url: str, *, method: str):  # noqa: ANN001
        _ = self, method
        return CrawlResponse(loaded_url=url, headers={}, content=b"")

    monkeypatch.setattr(
        GoogleStorageSource, "_get_blob_metadata_sync", _fake_blob_metadata
    )
    monkeypatch.setattr(GoogleStorageSource, "is_ready", _ready)
    monkeypatch.setattr(HttpUrlSource, "_crawl_once", _fake_crawl_once)

    changeset = await changeset_db.begin(
        actors=[json_actor], message="json list scan", status=OpStatus.IN_PROGRESS
    )
    status = await run_sources(
        sources=[json_actor], changeset=changeset, run_processors=False
    )
    await changeset.finalize(status=status)

    assert status == OpStatus.COMPLETED
    # Only the object in the configured bucket is read; the bucket itself is never listed.
    assert looked_up == ["scenarios/s1/docs/a.pdf"]

    assets = {
        asset.external_id: asset for asset in await asset_db.list_rows(order_by="id")
    }
    assert set(assets) == {"doc-key", "other-bucket", "web-key"}
    gcs_metadata = await md_db.for_asset(assets["doc-key"], include_removed=True)
    current = MetadataChanges(asset=assets["doc-key"], loaded=gcs_metadata).current()
    assert str(FILE_TYPE) in {str(key) for key in current.keys()}
