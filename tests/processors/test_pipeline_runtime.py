from __future__ import annotations

import asyncio
from typing import Callable, Iterable, Sequence

import pytest

from katalog.metadata import FILE_NAME, FILE_PATH, FILE_SIZE
from katalog.models import Asset, Metadata, OpStatus
from katalog.processors.base import Processor, ProcessorResult
from katalog.processors.runtime import process_asset
from tests.utils.pipeline_helpers import PipelineFixture


def make_processor(
    *,
    name: str,
    provider,
    dependencies: Iterable = (),
    outputs: Iterable = (),
    should_run_predicate: Callable[[set[str]], bool] | None = None,
    metadata_factory: Callable[[Asset], Sequence[Metadata]] | None = None,
):
    """Factory returning a processor instance with simple behavior for tests."""

    deps = frozenset(dependencies)
    outs = frozenset(outputs)

    class _Proc(Processor):
        dependencies = deps
        outputs = outs

        def __init__(self):
            super().__init__(provider=provider)
            self.runs = 0

        def should_run(self, asset, changes):
            if should_run_predicate is None:
                return True
            return should_run_predicate(set(changes or []))

        async def run(self, asset, changes):
            self.runs += 1
            metadata = metadata_factory(asset) if metadata_factory else []
            return ProcessorResult(metadata=list(metadata), status=OpStatus.COMPLETED)

    return _Proc()


@pytest.mark.asyncio
async def test_stage_dependency_triggers_following_stage(pipeline_db):
    ctx = await PipelineFixture.create()
    runs = []

    def stage1_meta(asset: Asset):
        runs.append("stage1")
        return [ctx.metadata(FILE_NAME, "example.txt")]

    def record_stage2():
        runs.append("stage2")

    stage1 = [
        make_processor(
            name="stage1",
            provider=ctx.provider,
            outputs=[FILE_NAME],
            metadata_factory=stage1_meta,
        )
    ]
    stage2 = [
        make_processor(
            name="stage2",
            provider=ctx.provider,
            dependencies=[FILE_NAME],
            outputs=[],
            should_run_predicate=lambda changes: FILE_NAME in changes,
            metadata_factory=lambda asset: record_stage2() or [],
        )
    ]

    upsert_calls = 0
    original_upsert = ctx.asset.upsert

    async def counted_upsert(*args, **kwargs):
        nonlocal upsert_calls
        upsert_calls += 1
        return await original_upsert(*args, **kwargs)

    ctx.asset.upsert = counted_upsert  # type: ignore[assignment]

    result = await process_asset(
        asset=ctx.asset,
        snapshot=ctx.snapshot,
        stages=[stage1, stage2],
        initial_changes=set(),
    )

    assert runs == ["stage1", "stage2"]
    assert FILE_NAME in result.changes
    assert upsert_calls == 1  # single flush at end
    assert stage1[0].runs == 1
    assert stage2[0].runs == 1


@pytest.mark.asyncio
async def test_processor_skipped_when_dependency_not_changed(pipeline_db):
    ctx = await PipelineFixture.create()

    proc = make_processor(
        name="size-dep",
        provider=ctx.provider,
        dependencies=[FILE_SIZE],
        should_run_predicate=lambda changes: FILE_SIZE in changes,
    )

    result = await process_asset(
        asset=ctx.asset,
        snapshot=ctx.snapshot,
        stages=[[proc]],
        initial_changes=set(),
    )

    assert proc.runs == 0
    assert result.changes == set()


@pytest.mark.asyncio
async def test_stage_processors_run_concurrently(pipeline_db):
    ctx = await PipelineFixture.create()
    start_a = asyncio.Event()
    start_b = asyncio.Event()
    completed: list[str] = []

    async def proc_a_run(asset, changes):
        start_a.set()
        await asyncio.wait_for(start_b.wait(), timeout=2)
        completed.append("a")
        return ProcessorResult(status=OpStatus.COMPLETED, metadata=[])

    async def proc_b_run(asset, changes):
        start_b.set()
        await asyncio.wait_for(start_a.wait(), timeout=2)
        completed.append("b")
        return ProcessorResult(status=OpStatus.COMPLETED, metadata=[])

    proc_a = make_processor(
        name="a",
        provider=ctx.provider,
        metadata_factory=None,
    )
    proc_b = make_processor(
        name="b",
        provider=ctx.provider,
        metadata_factory=None,
    )

    proc_a.run = proc_a_run  # type: ignore[assignment]
    proc_b.run = proc_b_run  # type: ignore[assignment]

    result = await asyncio.wait_for(
        process_asset(
            asset=ctx.asset,
            snapshot=ctx.snapshot,
            stages=[[proc_a, proc_b]],
            initial_changes=set(),
        ),
        timeout=3,
    )

    assert set(completed) == {"a", "b"}
    assert result.changes == set()
