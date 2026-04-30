from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
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
from katalog.processors.executors import ProcessorExecutorBundle
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
from katalog.db.sqlspec import forbid_db_access
from katalog.runtime.batch import get_batch_size, iter_batches
from katalog.config import current_db_url, current_workspace

ProcessorStage = list[Processor]


def _coerce_utc(dt: datetime) -> datetime:
    """Normalize datetimes for safe comparisons with changeset-id timestamps."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _should_run_coarse(
    processor: Processor,
    changes: MetadataChanges,
) -> bool:
    """Coarse skip contract for one processor on one asset.

    Run when either dependencies changed since last successful output or when
    `Actor.updated_at` is newer than the processor's latest output changeset on
    this asset. Skip only when both signals indicate no need to rerun.
    """

    actor = processor.actor
    actor_id = actor.id
    if actor_id is None:
        return True

    dependencies = set(processor.dependencies)
    outputs = set(processor.outputs)
    if not outputs:
        # Without declared outputs we cannot infer last successful run.
        return True

    deps_changed = changes.changed_since_actor(
        dependencies,
        actor_id=int(actor_id),
        actor_outputs=outputs,
    )
    if deps_changed:
        return True

    last_run_changeset_id = changes.latest_changeset_id(outputs, actor_id=int(actor_id))
    if last_run_changeset_id is None:
        return True

    actor_updated = actor.updated_at
    if actor_updated is None:
        return False

    last_run_started_at = datetime.fromtimestamp(
        int(last_run_changeset_id) / 1000.0,
        tz=UTC,
    )
    return _coerce_utc(actor_updated) > last_run_started_at


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
        result = await processor.run(changes)
        return result
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset_id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def _run_processor_with_mode(
    processor: Processor,
    changes: MetadataChanges,
    executors: ProcessorExecutorBundle,
) -> ProcessorResult:
    if processor.execution_mode == "threads":
        return await _run_processor_thread(processor, changes, executors)
    if processor.execution_mode == "cpu":
        return await _run_processor_process(processor, changes, executors)
    return await _run_processor(processor, changes)


def _run_processor_sync(
    processor: Processor,
    changes: MetadataChanges,
) -> ProcessorResult:
    asset = changes.asset
    asset_id = asset.id if asset is not None else None
    try:
        return asyncio.run(processor.run(changes))
    except Exception as e:
        msg = f"Processor {processor} failed for record {asset_id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg)


async def _run_processor_thread(
    processor: Processor,
    changes: MetadataChanges,
    executors: ProcessorExecutorBundle,
) -> ProcessorResult:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        executors.get_thread_executor(),
        _run_processor_sync,
        processor,
        changes,
    )


async def _run_processor_process(
    processor: Processor,
    changes: MetadataChanges,
    executors: ProcessorExecutorBundle,
) -> ProcessorResult:
    asset = changes.asset
    if asset is None:
        return ProcessorResult(status=OpStatus.ERROR, message="MetadataChanges.asset is missing")
    actor_payload = processor.actor.model_dump(mode="json")
    changes_payload = changes.model_dump(mode="json")
    registry_payload = dump_registry()
    app_context_payload = {
        "workspace": str(current_workspace()),
        "db_url": current_db_url(),
    }
    executors.record_cpu_processor(processor.actor.plugin_id)
    loop = asyncio.get_running_loop()
    result_payload = await loop.run_in_executor(
        executors.get_process_executor(),
        run_processor_in_process,
        actor_payload,
        changes_payload,
        registry_payload,
        app_context_payload,
    )
    normalized = normalize_processor_result_payload(result_payload)
    return ProcessorResult.model_validate(normalized)


async def _run_pipeline(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    executors: ProcessorExecutorBundle,
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

    with forbid_db_access():
        for stage in pipeline:
            coros: list[tuple[Processor, Awaitable[ProcessorResult]]] = []
            for processor in stage:
                if not force_run and not _should_run_coarse(processor, changes):
                    if stats:
                        stats.processings_skipped += 1
                    continue
                try:
                    should_run = True if force_run else processor.should_run(changes)
                except Exception:
                    logger.exception(
                        f"Processor {processor}.should_run failed for record {asset.id}"
                    )
                    continue
                if not should_run:
                    continue
                coros.append(
                    (processor, _run_processor_with_mode(processor, changes, executors))
                )
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
    executors: ProcessorExecutorBundle | None = None,
    force_run: bool = False,
) -> set[MetadataKey]:
    owns_executors = executors is None
    runtime_executors = executors or ProcessorExecutorBundle()
    cancelled = False
    try:
        updated = await _run_pipeline(
            changeset=changeset,
            pipeline=pipeline,
            changes=changes,
            executors=runtime_executors,
            force_run=force_run,
        )
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if owns_executors:
            runtime_executors.shutdown(cancelled=cancelled)
    md_db = get_metadata_repo()
    return await md_db.persist_changes(updated, changeset=changeset)


async def process_asset_collect(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes: MetadataChanges,
    executors: ProcessorExecutorBundle | None = None,
    force_run: bool = False,
) -> MetadataChanges:
    owns_executors = executors is None
    runtime_executors = executors or ProcessorExecutorBundle()
    cancelled = False
    try:
        return await _run_pipeline(
            changeset=changeset,
            pipeline=pipeline,
            changes=changes,
            executors=runtime_executors,
            force_run=force_run,
        )
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if owns_executors:
            runtime_executors.shutdown(cancelled=cancelled)


def _has_custom_batch_run(processor: Processor) -> bool:
    return processor.__class__.run_batch is not Processor.run_batch


async def process_batch_collect(
    *,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    changes_batch: list[MetadataChanges],
    executors: ProcessorExecutorBundle | None = None,
    force_run: bool = False,
) -> list[MetadataChanges]:
    """Run a dependency-sorted processor pipeline over one hydrated asset batch."""
    if not changes_batch:
        return changes_batch
    stats = changeset.stats
    if stats is None:
        stats = ChangesetStats()
        changeset.stats = stats
    for changes in changes_batch:
        if changes.asset is not None:
            stats.assets_processed += 1

    owns_executors = executors is None
    runtime_executors = executors or ProcessorExecutorBundle()
    cancelled = False
    try:
        with forbid_db_access():
            for stage in pipeline:
                for processor in stage:
                    eligible: list[tuple[int, MetadataChanges]] = []
                    for idx, changes in enumerate(changes_batch):
                        asset = changes.asset
                        if asset is None:
                            continue
                        if not force_run and not _should_run_coarse(processor, changes):
                            stats.processings_skipped += 1
                            continue
                        try:
                            should_run = True if force_run else processor.should_run(changes)
                        except Exception:
                            logger.exception(
                                "Processor {processor}.should_run failed for record {asset_id}",
                                processor=processor,
                                asset_id=asset.id,
                            )
                            continue
                        if not should_run:
                            continue
                        eligible.append((idx, changes))
                    if not eligible:
                        continue
                    stats.processings_started += len(eligible)
                    results_by_idx: dict[int, ProcessorResult] = {}
                    if _has_custom_batch_run(processor):
                        run_inputs = [changes for _, changes in eligible]
                        try:
                            run_results = await processor.run_batch(run_inputs)
                            if len(run_results) != len(run_inputs):
                                raise RuntimeError(
                                    f"run_batch length mismatch for {processor}: "
                                    f"{len(run_results)} != {len(run_inputs)}"
                                )
                            for (idx, _changes), result in zip(eligible, run_results, strict=True):
                                results_by_idx[idx] = result
                        except Exception as exc:  # noqa: BLE001
                            logger.exception(
                                "Processor {processor}.run_batch failed: {error}",
                                processor=processor,
                                error=str(exc),
                            )
                            for idx, changes in eligible:
                                asset_id = changes.asset.id if changes.asset is not None else None
                                results_by_idx[idx] = ProcessorResult(
                                    status=OpStatus.ERROR,
                                    message=(
                                        f"Processor {processor} failed for record {asset_id}: {exc}"
                                    ),
                                )
                    else:
                        coros: list[tuple[int, Awaitable[ProcessorResult]]] = [
                            (
                                idx,
                                _run_processor_with_mode(processor, changes, runtime_executors),
                            )
                            for idx, changes in eligible
                        ]
                        run_results = await asyncio.gather(*(coro for _, coro in coros))
                        for (idx, _), result in zip(coros, run_results, strict=True):
                            results_by_idx[idx] = result

                    for idx, result in results_by_idx.items():
                        status = result.status
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
                        if status in (OpStatus.CANCELED, OpStatus.ERROR, OpStatus.SKIPPED):
                            continue
                        changes_batch[idx].add(result.metadata)
        return changes_batch
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if owns_executors:
            runtime_executors.shutdown(cancelled=cancelled)


async def _process_batch(
    *,
    batch_assets: list[Asset],
    batch_label: str,
    changeset: Changeset,
    pipeline: Sequence[ProcessorStage],
    executors: ProcessorExecutorBundle,
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
                    executors=executors,
                    force_run=False,
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
    executors = ProcessorExecutorBundle()
    cancelled = False
    try:
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
                    executors=executors,
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
                    executors=executors,
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
                executors=executors,
                stats=stats,
                metadata_repo=md_db,
            )
            offset += batch_size
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        executors.shutdown(cancelled=cancelled)
