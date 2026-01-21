from __future__ import annotations

import asyncio
from typing import Callable, Iterable, Sequence

import pytest

from katalog.constants.metadata import FILE_NAME, FILE_SIZE, HASH_MD5
from katalog.models import Asset, Metadata, MetadataChangeSet, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult
from katalog.processors.runtime import process_asset
from katalog.processors.md5_hash import MD5HashProcessor
from tests.utils.pipeline_helpers import PipelineFixture


def make_processor(
    *,
    name: str,
    actor,
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
            super().__init__(actor=actor)
            self.runs = 0

        def should_run(self, asset, change_set):
            if should_run_predicate is None:
                return True
            return should_run_predicate(set(change_set.changed_keys()))

        async def run(self, asset, change_set):
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
            actor=ctx.actor,
            outputs=[FILE_NAME],
            metadata_factory=stage1_meta,
        )
    ]
    stage2 = [
        make_processor(
            name="stage2",
            actor=ctx.actor,
            dependencies=[FILE_NAME],
            outputs=[],
            should_run_predicate=lambda changes: FILE_NAME in changes,
            metadata_factory=lambda asset: record_stage2() or [],
        )
    ]

    change_set = MetadataChangeSet(await ctx.asset.load_metadata())
    changes = await process_asset(
        asset=ctx.asset,
        changeset=ctx.changeset,
        stages=[stage1, stage2],
        change_set=change_set,
    )

    assert runs == ["stage1", "stage2"]
    assert FILE_NAME in changes
    assert FILE_NAME in change_set.changed_keys()
    assert stage1[0].runs == 1
    assert stage2[0].runs == 1


@pytest.mark.asyncio
async def test_processor_skipped_when_dependency_not_changed(pipeline_db):
    ctx = await PipelineFixture.create()

    proc = make_processor(
        name="size-dep",
        actor=ctx.actor,
        dependencies=[FILE_SIZE],
        should_run_predicate=lambda changes: FILE_SIZE in changes,
    )

    change_set = MetadataChangeSet(await ctx.asset.load_metadata())
    changes = await process_asset(
        asset=ctx.asset, changeset=ctx.changeset, stages=[[proc]], change_set=change_set
    )

    assert proc.runs == 0
    assert change_set.changed_keys() == set()
    assert changes == set()


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
        actor=ctx.actor,
        metadata_factory=None,
    )
    proc_b = make_processor(
        name="b",
        actor=ctx.actor,
        metadata_factory=None,
    )

    proc_a.run = proc_a_run  # type: ignore[assignment]
    proc_b.run = proc_b_run  # type: ignore[assignment]

    change_set = MetadataChangeSet(await ctx.asset.load_metadata())
    await asyncio.wait_for(
        process_asset(
            asset=ctx.asset,
            changeset=ctx.changeset,
            stages=[[proc_a, proc_b]],
            change_set=change_set,
        ),
        timeout=3,
    )

    assert set(completed) == {"a", "b"}
    assert change_set.changed_keys() == set()


@pytest.mark.asyncio
async def test_md5_skips_when_hash_already_present(pipeline_db):
    ctx = await PipelineFixture.create()
    md5_processor = MD5HashProcessor(actor=ctx.actor)

    # Seed an existing hash in DB and cache
    existing_md5 = make_metadata(HASH_MD5, "abc", actor_id=ctx.actor.id)
    existing_md5.asset = ctx.asset
    existing_md5.changeset = ctx.changeset
    await ctx.asset.save_record(changeset=ctx.changeset, actor=ctx.actor)
    change_set = MetadataChangeSet(
        loaded=await ctx.asset.load_metadata(), staged=[existing_md5]
    )
    await change_set.persist(asset=ctx.asset, changeset=ctx.changeset)

    change_set = MetadataChangeSet(await ctx.asset.load_metadata())
    changes = await process_asset(
        asset=ctx.asset,
        changeset=ctx.changeset,
        stages=[[md5_processor]],
        change_set=change_set,
    )

    assert md5_processor.should_run(ctx.asset, change_set) is False
    assert HASH_MD5 not in change_set.changed_keys()
    assert HASH_MD5 not in changes
