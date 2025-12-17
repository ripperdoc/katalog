from __future__ import annotations

import pytest

from katalog.db import Database
from katalog.models import FILE_NAME, Asset, SnapshotStats, make_metadata
from katalog.processors import runtime
from katalog.processors.base import Processor, ProcessorResult


class DummyProcessor(Processor):
    PLUGIN_ID = "test.processor"
    outputs = frozenset({FILE_NAME})

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.provider_id = "processor:dummy"

    def should_run(
        self, record, changes, database=None
    ) -> bool:  # pragma: no cover - simple stub
        return True

    async def run(self, record, changes) -> ProcessorResult:
        return ProcessorResult(
            metadata=[make_metadata(self.provider_id, FILE_NAME, "dummy.txt")]
        )


def _setup_database() -> Database:
    db = Database(":memory:")
    db.initialize_schema()
    db.ensure_source(
        "source:test",
        title="Test Source",
        plugin_id="tests.source",
        config={},
        provider_type="source",
    )
    return db


@pytest.mark.asyncio
async def test_runtime_process_handles_processor_result_metadata():
    db = _setup_database()
    asset = Asset(
        id="asset-1",
        provider_id="source:test",
        canonical_uri="test://asset-1",
    )
    snapshot = db.begin_snapshot("source:test")
    stats = SnapshotStats()

    configs = [
        {
            "class": "tests.processors.test_runtime.DummyProcessor",
            "id": "dummy",
        }
    ]

    stages = runtime.sort_processors(configs, database=db)

    task_result = await runtime.process_asset(
        record=record,
        snapshot=snapshot,
        database=db,
        stages=stages,
        initial_changes=None,
        stats=stats,
    )
    assert str(FILE_NAME) in task_result.changes
    entries = db.get_latest_metadata_for_file(asset.id, metadata_key=FILE_NAME)
    assert [entry.value for entry in entries if not entry.removed] == ["dummy.txt"]
    assert stats.processings_started == 1
    assert stats.processings_completed == 1
    db.finalize_snapshot(snapshot, status="full", stats=stats)
    db.close()
