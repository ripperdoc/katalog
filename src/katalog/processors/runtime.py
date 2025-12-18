from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from loguru import logger

from katalog.models import (
    Asset,
    Metadata,
    MetadataKey,
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    SnapshotStats,
)
from katalog.processors.base import (
    Processor,
    ProcessorResult,
    make_processor_instance,
)


ProcessorStage = list[Processor]


@dataclass(slots=True)
class ProcessorTaskResult:
    """Summarizes the outcome of running the processor pipeline for one asset."""

    changes: set[str]
    failures: int = 0


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
    processor: Processor, asset: Asset, changes: set[str]
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
    stats: SnapshotStats | None = None,
) -> ProcessorTaskResult:
    """Run processors per stage, returning the union of all observed changes."""

    if not stages:
        return ProcessorTaskResult(changes=set(), failures=0)

    changes = set(initial_changes or [])
    failed_runs = 0
    for stage in stages:
        coros: list[tuple[Processor, Any]] = []
        for processor in stage:
            try:
                should_run = processor.should_run(asset, changes)
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
        stage_changes = await asset.upsert(
            snapshot=snapshot, metadata=stage_metadata, stats=stats
        )
        if stage_changes:
            changes.update(stage_changes)
    return ProcessorTaskResult(changes=changes, failures=failed_runs)


async def drain_processor_tasks(tasks: list[asyncio.Task[Any]]) -> tuple[int, int]:
    if not tasks:
        return 0, 0
    results = await asyncio.gather(*tasks, return_exceptions=True)
    modified = 0
    failures = 0
    for result in results:
        if isinstance(result, Exception):
            logger.opt(exception=result).error("Processor task failed")
            failures += 1
            continue
        if isinstance(result, ProcessorTaskResult):
            if result.changes:
                modified += 1
            failures += result.failures
            continue
        if result:
            modified += 1
    tasks.clear()
    return modified, failures
