from __future__ import annotations

import asyncio
import os
from typing import Awaitable, Sequence

from loguru import logger

from katalog.models import (
    Asset,
    Metadata,
    MetadataKey,
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    MetadataChangeSet,
)
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    make_processor_instance,
)


DEFAULT_PROCESSOR_CONCURRENCY = max(4, (os.cpu_count() or 4))


ProcessorStage = list[Processor]


async def sort_processors() -> list[ProcessorStage]:
    """Return processors layered via Kahn topological sorting."""

    providers = await Provider.filter(type=ProviderType.PROCESSOR).order_by("id")
    if not providers:
        logger.warning("No processor providers found")
        return []

    processors_by_name: dict[str, Processor] = {}
    order_by_name: dict[str, tuple[int, int]] = {}
    dependencies_by_name: dict[str, frozenset[MetadataKey]] = {}
    outputs_by_name: dict[str, frozenset[MetadataKey]] = {}

    for provider in providers:
        processor = make_processor_instance(provider)
        # Used when persisting produced metadata.
        name = provider.name
        processors_by_name[name] = processor
        cfg = provider.config or {}
        order = int(cfg.get("order") or 0)
        seq = int(cfg.get("_sequence") or provider.id)
        order_by_name[name] = (order, seq)
        dependencies_by_name[name] = processor.dependencies
        outputs_by_name[name] = processor.outputs

    field_to_producers: dict[MetadataKey, set[str]] = {}
    for name, outputs in outputs_by_name.items():
        for output in outputs:
            field_to_producers.setdefault(output, set()).add(name)

    remaining: dict[str, set[str]] = {}
    for name, deps in dependencies_by_name.items():
        producers: set[str] = set()
        for dependency in deps:
            producers.update(field_to_producers.get(dependency, set()))
        remaining[name] = producers

    stages: list[ProcessorStage] = []
    while remaining:
        ready = sorted(
            [name for name, deps in remaining.items() if not deps],
            key=lambda n: order_by_name.get(n, (0, 0)),
        )
        if not ready:
            raise RuntimeError(f"Circular dependency detected: {remaining}")
        stages.append([processors_by_name[name] for name in ready])
        for name in ready:
            remaining.pop(name, None)
        for deps in remaining.values():
            deps.difference_update(ready)
    return stages


async def _run_processor(
    processor: Processor,
    asset: Asset,
    change_set,
) -> ProcessorResult:
    try:
        logger.debug(f"Running processor {processor} for record {asset.id}")
        result = await processor.run(asset, change_set)
        return result
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset.id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def process_asset(
    *,
    asset: Asset,
    snapshot: Snapshot,
    stages: Sequence[ProcessorStage],
    change_set: MetadataChangeSet,
    force_run: bool = False,
) -> set[MetadataKey]:
    stats = snapshot.stats
    failed_runs = 0

    for stage in stages:
        coros: list[tuple[Processor, Awaitable[ProcessorResult]]] = []
        for processor in stage:
            try:
                should_run = (
                    True if force_run else processor.should_run(asset, change_set)
                )
            except Exception:
                logger.exception(
                    f"Processor {processor}.should_run failed for record {asset.id}"
                )
                continue
            if not should_run:
                continue
            coros.append((processor, _run_processor(processor, asset, change_set)))
            if stats:
                stats.processings_started += 1
        if not coros:
            continue
        results: list[ProcessorResult] = await asyncio.gather(
            *(coro for _, coro in coros)
        )
        stage_metadata: list[Metadata] = []
        for result in results:
            status = result.status
            if stats:
                if status == OpStatus.COMPLETED:
                    stats.processings_completed += 1
                elif status == OpStatus.PARTIAL:
                    stats.processings_partial += 1
                elif status == OpStatus.CANCELED:
                    stats.processings_cancelled += 1
                elif status == OpStatus.SKIPPED:
                    stats.processings_skipped += 1
                elif status == OpStatus.ERROR:
                    stats.processings_error += 1
            if status in (OpStatus.CANCELED, OpStatus.ERROR):
                failed_runs += 1
                continue
            if status == OpStatus.SKIPPED:
                continue
            for meta in result.metadata:
                stage_metadata.append(meta)
        # Add all produced metadata to the change set for the next stage.
        change_set.add(stage_metadata)
    return await change_set.persist(asset=asset, snapshot=snapshot)


async def enqueue_asset_processing(
    *,
    asset: Asset,
    snapshot: Snapshot,
    stages: Sequence[ProcessorStage],
    change_set: MetadataChangeSet,
    force_run: bool = False,
) -> None:
    """Schedule processor pipeline for an asset with bounded concurrency."""

    if not stages:
        return

    async def runner() -> set[MetadataKey]:
        async with snapshot.semaphore:
            snapshot.stats.assets_processed += 1
            return await process_asset(
                asset=asset,
                snapshot=snapshot,
                stages=stages,
                change_set=change_set,
                force_run=force_run,
            )

    snapshot.tasks.append(asyncio.create_task(runner()))


async def run_processors(*, snapshot: Snapshot, assets: list[Asset]):
    """Run processors for a list of assets belonging to one provider."""
    processor_pipeline = await sort_processors()

    for asset in assets:
        snapshot.stats.assets_seen += 1
        loaded_metadata = await asset.load_metadata()
        change_set = MetadataChangeSet(loaded=loaded_metadata)
        await enqueue_asset_processing(
            asset=asset,
            snapshot=snapshot,
            stages=processor_pipeline,
            change_set=change_set,
            # Run all processors regardless of should_run because we have no prior info on changes
            force_run=True,
        )
