from __future__ import annotations

import asyncio
import os
from typing import Any, Iterable, Sequence

from loguru import logger

from katalog.models import (
    Asset,
    Metadata,
    MetadataKey,
    OpStatus,
    ProcessorTaskResult,
    Provider,
    ProviderType,
    Snapshot,
)
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    make_processor_instance,
)
from katalog.metadata import get_metadata_def_by_id


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
    changes: set[str],
) -> tuple[ProcessorResult, bool]:
    try:
        logger.debug(f"Running processor {processor} for record {asset.id}")
        result = await processor.run(asset, changes)
        return result, False
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset.id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg), True


async def process_asset(
    *,
    asset: Asset,
    snapshot: Snapshot,
    stages: Sequence[ProcessorStage],
    initial_changes: Iterable[str] | None = None,
    force_run: bool = False,
) -> ProcessorTaskResult:
    """Run processors per stage, returning the union of all observed changes."""

    if not stages:
        return ProcessorTaskResult(changes=set(initial_changes or []), failures=0)
    stats = snapshot.stats
    changes = set(initial_changes or [])
    failed_runs = 0
    pending_metadata: list[Metadata] = []
    existing_metadata = await asset.load_metadata()
    seen_metadata: set[tuple[int, int, Any]] = {
        (int(md.metadata_key_id), int(md.provider_id), md.value)
        for md in existing_metadata
    }
    for stage in stages:
        coros: list[tuple[Processor, Any]] = []
        for processor in stage:
            try:
                should_run = True if force_run else processor.should_run(asset, changes)
            except Exception:
                logger.exception(
                    f"Processor {processor}.should_run failed for record {asset.id}"
                )
                continue
            if not should_run:
                continue
            coros.append((processor, _run_processor(processor, asset, changes)))
            if stats:
                stats.processings_started += 1
        if not coros:
            continue
        results = await asyncio.gather(*(coro for _, coro in coros))
        stage_metadata: list[Metadata] = []
        for (processor, _), outcome in zip(coros, results):
            produced, failed = outcome
            if failed:
                failed_runs += 1
                if stats:
                    stats.processings_error += 1
                continue
            status = produced.status
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
            for meta in produced.metadata:
                stage_metadata.append(meta)
        if not stage_metadata:
            continue
        new_entries: list[Metadata] = []
        for md in stage_metadata:
            key = (int(md.metadata_key_id), int(md.provider_id), md.value)
            if key in seen_metadata:
                continue
            seen_metadata.add(key)
            new_entries.append(md)
        if not new_entries:
            continue
        pending_metadata.extend(new_entries)
        for md in new_entries:
            changed_key = get_metadata_def_by_id(int(md.metadata_key_id)).key
            changes.add(changed_key)
    if pending_metadata:
        final_changes = await asset.upsert(snapshot=snapshot, metadata=pending_metadata)
        changes.update(final_changes)
    return ProcessorTaskResult(changes=changes, failures=failed_runs)


async def enqueue_asset_processing(
    *,
    asset: Asset,
    snapshot: Snapshot,
    stages: Sequence[ProcessorStage],
    tasks: list[asyncio.Task[Any]],
    semaphore: asyncio.Semaphore,
    initial_changes: Iterable[str] | None = None,
    force_run: bool = False,
) -> None:
    """Schedule processor pipeline for an asset with bounded concurrency."""

    if not stages:
        return

    async def runner() -> ProcessorTaskResult:
        async with semaphore:
            return await process_asset(
                asset=asset,
                snapshot=snapshot,
                stages=stages,
                initial_changes=initial_changes,
                force_run=force_run,
            )

    tasks.append(asyncio.create_task(runner()))
