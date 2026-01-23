from __future__ import annotations

import asyncio
import os
from typing import Awaitable, Sequence, cast

from loguru import logger

from katalog.models import (
    Asset,
    Metadata,
    OpStatus,
    Actor,
    ActorType,
    Changeset,
    MetadataChanges,
)
from katalog.processors.base import Processor, ProcessorResult
from katalog.plugins.registry import get_actor_instance

DEFAULT_PROCESSOR_CONCURRENCY = max(4, (os.cpu_count() or 4))


ProcessorStage = list[Processor]


async def sort_processors(
    actor_ids: list[int] | None = None,
) -> tuple[list[ProcessorStage], list[Actor]]:
    """Return processors layered via Kahn topological sorting, plus actor records."""

    query = Actor.filter(type=ActorType.PROCESSOR, disabled=False)
    if actor_ids:
        query = query.filter(id__in=sorted(set(actor_ids)))
    actors = await query.order_by("id")
    if not actors:
        logger.warning("No processor actors found")
        return [], []

    processors_by_name: dict[str, Processor] = {}
    order_by_name: dict[str, tuple[int, int]] = {}
    dependencies_by_name: dict[str, frozenset[MetadataKey]] = {}
    outputs_by_name: dict[str, frozenset[MetadataKey]] = {}

    for actor in actors:
        processor = cast(Processor, await get_actor_instance(actor))
        # Used when persisting produced metadata.
        name = actor.name
        processors_by_name[name] = processor
        cfg = actor.config or {}
        order = int(cfg.get("order") or 0)
        seq = int(cfg.get("_sequence") or actor.id)
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
    return stages, actors


async def _run_processor(
    processor: Processor,
    asset: Asset,
    changes,
) -> ProcessorResult:
    try:
        logger.debug(f"Running processor {processor} for record {asset.id}")
        result = await processor.run(asset, changes)
        return result
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset.id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def process_asset(
    *,
    asset: Asset,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    force_run: bool = False,
) -> set[MetadataKey]:
    stats = changeset.stats
    failed_runs = 0
    changeset.stats.assets_processed += 1

    for stage in pipeline:
        coros: list[tuple[Processor, Awaitable[ProcessorResult]]] = []
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
        changes.add(stage_metadata)
    return await changes.persist(asset=asset, changeset=changeset)


async def run_processors(
    *,
    changeset: Changeset,
    assets: list[Asset],
    pipeline: Sequence[ProcessorStage],
):
    """Run processors for a list of assets belonging to one actor."""

    for asset in assets:
        changeset.stats.assets_seen += 1
        changeset.stats.assets_saved += 1
        loaded_metadata = await asset.load_metadata()
        changes = MetadataChanges(loaded=loaded_metadata)

        changeset.enqueue(
            process_asset(
                asset=asset,
                changeset=changeset,
                pipeline=pipeline,
                changes=changes,
                force_run=True,
            )
        )
