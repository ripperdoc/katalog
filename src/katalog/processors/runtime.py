from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Awaitable, Sequence, cast

from loguru import logger

from katalog.constants.metadata import MetadataKey
from katalog.models import (
    Asset,
    Metadata,
    OpStatus,
    Actor,
    ActorType,
    Changeset,
    MetadataChanges,
    ChangesetStats,
)
from katalog.processors.base import Processor, ProcessorResult
from katalog.processors.process_executor import run_processor_in_process
from katalog.processors.serialization import (
    dump_registry,
    normalize_processor_result_payload,
)
from katalog.plugins.registry import get_actor_instance
from katalog.db.assets import get_asset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.db.actors import get_actor_repo
from katalog.runtime.batch import get_batch_size, iter_batches

DEFAULT_PROCESSOR_CONCURRENCY = max(4, (os.cpu_count() or 4))
DEFAULT_THREAD_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY
DEFAULT_PROCESS_CONCURRENCY = DEFAULT_PROCESSOR_CONCURRENCY

_THREAD_EXECUTOR: ThreadPoolExecutor | None = None
_PROCESS_EXECUTOR: ProcessPoolExecutor | None = None


def _get_thread_executor() -> ThreadPoolExecutor:
    global _THREAD_EXECUTOR
    if _THREAD_EXECUTOR is None:
        _THREAD_EXECUTOR = ThreadPoolExecutor(max_workers=DEFAULT_THREAD_CONCURRENCY)
    return _THREAD_EXECUTOR


def _get_process_executor() -> ProcessPoolExecutor:
    global _PROCESS_EXECUTOR
    if _PROCESS_EXECUTOR is None:
        _PROCESS_EXECUTOR = ProcessPoolExecutor(max_workers=DEFAULT_PROCESS_CONCURRENCY)
    return _PROCESS_EXECUTOR


ProcessorStage = list[Processor]


async def sort_processors(
    actor_ids: list[int] | None = None,
) -> tuple[list[ProcessorStage], list[Actor]]:
    """Return processors layered via Kahn topological sorting, plus actor records."""

    filters = {"type": ActorType.PROCESSOR, "disabled": False}
    if actor_ids:
        filters["id__in"] = sorted(set(actor_ids))
    db = get_actor_repo()
    actors = await db.list_rows(order_by="id", **filters)
    if not actors:
        logger.warning("No processor actors found")
        return [], []

    processors_by_name: dict[str, Processor] = {}
    order_by_name: dict[str, tuple[int, int]] = {}
    dependencies_by_name: dict[str, frozenset[MetadataKey]] = {}
    outputs_by_name: dict[str, frozenset[MetadataKey]] = {}

    for actor in actors:
        processor = cast(Processor, await get_actor_instance(actor))
        ready, reason = await processor.is_ready()
        if not ready:
            detail = reason or "unknown reason"
            raise RuntimeError(
                f"Processor {actor.name} ({actor.plugin_id}) is not ready: {detail}"
            )
        # Used when persisting produced metadata.
        name = actor.name
        processors_by_name[name] = processor
        cfg = actor.config or {}
        order = int(cfg.get("order") or 0)
        seq = int(cfg.get("_sequence") or (actor.id or 0))
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
    changes,
) -> ProcessorResult:
    asset = changes.asset
    asset_id = asset.id if asset is not None else None
    try:
        logger.debug(f"Running processor {processor} for record {asset_id}")
        result = await processor.run(changes)
        return result
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset_id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def _run_processor_with_mode(
    processor: Processor,
    changes: MetadataChanges,
) -> ProcessorResult:
    if processor.execution_mode == "threads":
        return await _run_processor_thread(processor, changes)
    if processor.execution_mode == "cpu":
        return await _run_processor_process(processor, changes)
    return await _run_processor(processor, changes)


def _run_processor_sync(
    processor: Processor,
    changes: MetadataChanges,
) -> ProcessorResult:
    asset = changes.asset
    asset_id = asset.id if asset is not None else None
    try:
        logger.debug(f"Running processor {processor} for record {asset_id}")
        return asyncio.run(processor.run(changes))
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset_id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def _run_processor_thread(
    processor: Processor,
    changes: MetadataChanges,
) -> ProcessorResult:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _get_thread_executor(),
        _run_processor_sync,
        processor,
        changes,
    )


async def _run_processor_process(
    processor: Processor,
    changes: MetadataChanges,
) -> ProcessorResult:
    asset = changes.asset
    if asset is None:
        return ProcessorResult(status=OpStatus.ERROR, message="MetadataChanges.asset is missing")
    actor_payload = processor.actor.model_dump(mode="json")
    changes_payload = changes.model_dump(mode="json")
    registry_payload = dump_registry()
    loop = asyncio.get_running_loop()
    result_payload = await loop.run_in_executor(
        _get_process_executor(),
        run_processor_in_process,
        actor_payload,
        changes_payload,
        registry_payload,
    )
    normalized = normalize_processor_result_payload(result_payload)
    return ProcessorResult.model_validate(normalized)


async def _run_pipeline(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    force_run: bool = False,
) -> MetadataChanges:
    asset = changes.asset
    if asset is None:
        raise ValueError("MetadataChanges.asset is required for processor pipeline")
    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats
    stats.assets_processed += 1

    for stage in pipeline:
        coros: list[tuple[Processor, Awaitable[ProcessorResult]]] = []
        for processor in stage:
            try:
                should_run = True if force_run else processor.should_run(changes)
            except Exception:
                logger.exception(
                    f"Processor {processor}.should_run failed for record {asset.id}"
                )
                continue
            if not should_run:
                continue
            coros.append((processor, _run_processor_with_mode(processor, changes)))
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
                continue
            if status == OpStatus.SKIPPED:
                continue
            for meta in result.metadata:
                stage_metadata.append(meta)
        changes.add(stage_metadata)
    return changes




async def process_asset(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    force_run: bool = False,
) -> set[MetadataKey]:
    updated = await _run_pipeline(
        changeset=changeset,
        pipeline=pipeline,
        changes=changes,
        force_run=force_run,
    )
    md_db = get_metadata_repo()
    return await md_db.persist_changes(updated, changeset=changeset)


async def process_asset_collect(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    force_run: bool = False,
) -> MetadataChanges:
    return await _run_pipeline(
        changeset=changeset,
        pipeline=pipeline,
        changes=changes,
        force_run=force_run,
    )


async def _process_batch(
    *,
    batch_assets: list[Asset],
    batch_label: str,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    stats: ChangesetStats,
    metadata_repo,
) -> None:
    asset_ids_batch = [int(asset.id) for asset in batch_assets if asset.id is not None]
    read_started = time.perf_counter()
    logger.info(
        "Processor batch read start batch={batch} assets={assets}",
        batch=batch_label,
        assets=len(batch_assets),
    )
    metadata_by_asset = await metadata_repo.for_assets(
        asset_ids_batch, include_removed=True
    )
    read_elapsed = time.perf_counter() - read_started
    metadata_count = sum(len(rows) for rows in metadata_by_asset.values())
    logger.info(
        "Processor batch read done batch={batch} assets={assets} metadata={metadata} seconds={seconds:.2f}",
        batch=batch_label,
        assets=len(batch_assets),
        metadata=metadata_count,
        seconds=read_elapsed,
    )

    tasks: list[asyncio.Task[MetadataChanges]] = []
    for asset in batch_assets:
        stats.assets_seen += 1
        stats.assets_saved += 1
        loaded_metadata = metadata_by_asset.get(int(asset.id), []) if asset.id else []
        changes = MetadataChanges(asset=asset, loaded=loaded_metadata)
        tasks.append(
            changeset.enqueue(
                process_asset_collect(
                    changeset=changeset,
                    pipeline=pipeline,
                    changes=changes,
                    force_run=True,
                )
            )
        )

    changes_list = await asyncio.gather(*tasks)
    persist_started = time.perf_counter()
    normal_rows, search_rows, delete_rows = await metadata_repo.persist_changes_batch(
        changeset,
        changes_list,
        metadata_by_asset,
    )
    persist_elapsed = time.perf_counter() - persist_started
    logger.info(
        "Processor batch persist done batch={batch} assets={assets} rows={rows} search_upserts={upserts} search_deletes={deletes} seconds={seconds:.2f}",
        batch=batch_label,
        assets=len(batch_assets),
        rows=normal_rows,
        upserts=search_rows,
        deletes=delete_rows,
        seconds=persist_elapsed,
    )




async def do_run_processors(
    *,
    changeset: Changeset,
    assets: list[Asset] | None,
    asset_ids: list[int] | None = None,
    pipeline: Sequence[ProcessorStage],
):
    """Run processors for a list of assets belonging to one actor."""
    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats
    batch_size = get_batch_size()
    logger.info("Processor run batch_size={batch_size}", batch_size=batch_size)

    db = get_asset_repo()
    md_db = get_metadata_repo()
    batch_index = 0

    if asset_ids:
        for id_batch in iter_batches(sorted(asset_ids), batch_size):
            batch_assets = await db.list_rows(order_by="id", id__in=id_batch)
            if len(batch_assets) != len(id_batch):
                missing = set(id_batch) - {int(a.id) for a in batch_assets if a.id}
                raise ValueError(f"Asset ids not found: {sorted(missing)}")
            batch_index += 1
            await _process_batch(
                batch_assets=batch_assets,
                batch_label=f"{batch_index}",
                changeset=changeset,
                pipeline=pipeline,
                stats=stats,
                metadata_repo=md_db,
            )
        return

    if assets is not None:
        for batch_assets in iter_batches(assets, batch_size):
            batch_index += 1
            await _process_batch(
                batch_assets=batch_assets,
                batch_label=f"{batch_index}",
                changeset=changeset,
                pipeline=pipeline,
                stats=stats,
                metadata_repo=md_db,
            )
        return

    offset = 0
    while True:
        batch_assets = await db.list_rows(
            order_by="id",
            limit=batch_size,
            offset=offset,
        )
        if not batch_assets:
            break
        batch_index += 1
        await _process_batch(
            batch_assets=batch_assets,
            batch_label=f"{batch_index}",
            changeset=changeset,
            pipeline=pipeline,
            stats=stats,
            metadata_repo=md_db,
        )
        offset += batch_size
