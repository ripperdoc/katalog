from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from loguru import logger

from katalog.db import Database, Snapshot
from katalog.models import AssetRecord, Metadata, MetadataKey
from katalog.processors.base import Processor, ProcessorResult, ProcessorStatus
from katalog.utils.utils import import_plugin_class


@dataclass(slots=True)
class ProcessorEntry:
    """Runtime wrapper that keeps processor metadata handy."""

    name: str
    provider_id: str
    plugin_id: str
    instance: Processor
    dependencies: frozenset[MetadataKey]
    outputs: frozenset[MetadataKey]
    order: int


ProcessorStage = list[ProcessorEntry]


@dataclass(slots=True)
class ProcessorTaskResult:
    """Summarizes the outcome of running the processor pipeline for one record."""

    changes: set[str]
    failures: int = 0


def _instantiate_processor(
    config: dict[str, Any], index: int, database: Database | None
) -> ProcessorEntry:
    class_path = config.get("class")
    if not class_path:
        raise ValueError("Each processor config must include a 'class' field")
    ProcessorClass = import_plugin_class(class_path)
    kwargs = {k: v for k, v in config.items() if k not in {"class", "id", "order"}}
    if database is not None:
        kwargs.setdefault("database", database)
    instance = ProcessorClass(**kwargs)
    name = config.get("id") or f"{ProcessorClass.__name__}:{index}"
    plugin_id = getattr(ProcessorClass, "PLUGIN_ID", ProcessorClass.__module__)
    provider_id = config.get("provider_id") or f"processor:{name}"
    if database is not None:
        database.ensure_source(
            provider_id,
            title=config.get("title") or f"Processor {name}",
            plugin_id=plugin_id,
            config=config,
            provider_type="processor",
        )
    if not hasattr(instance, "provider_id"):
        setattr(instance, "provider_id", provider_id)
    order_value = config.get("order")
    order = int(order_value) if order_value is not None else 0
    return ProcessorEntry(
        name=name,
        provider_id=provider_id,
        plugin_id=plugin_id,
        instance=instance,
        dependencies=ProcessorClass.dependencies,
        outputs=ProcessorClass.outputs,
        order=order,
    )


def sort_processors(
    configs: Iterable[dict[str, Any]],
    database: Database | None = None,
) -> list[ProcessorStage]:
    """Return processors layered via Kahn topological sorting."""

    entries = [
        _instantiate_processor(cfg, idx, database) for idx, cfg in enumerate(configs)
    ]
    if not entries:
        return []

    # Build lookup helpers
    index_by_name = {entry.name: idx for idx, entry in enumerate(entries)}
    entry_map = {entry.name: entry for entry in entries}

    field_to_producers: dict[MetadataKey, set[str]] = {}
    for entry in entries:
        for output in entry.outputs:
            field_to_producers.setdefault(output, set()).add(entry.name)

    remaining: dict[str, set[str]] = {}
    for entry in entries:
        deps: set[str] = set()
        for dependency in entry.dependencies:
            deps.update(field_to_producers.get(dependency, set()))
        remaining[entry.name] = deps

    stages: list[ProcessorStage] = []
    while remaining:
        ready = sorted(
            [name for name, deps in remaining.items() if not deps],
            key=lambda name: (entries[index_by_name[name]].order, index_by_name[name]),
        )
        if not ready:
            raise RuntimeError(f"Circular dependency detected: {remaining}")
        stage: ProcessorStage = [entry_map[name] for name in ready]
        stages.append(stage)
        for name in ready:
            remaining.pop(name, None)
        for deps in remaining.values():
            deps.difference_update(ready)
    return stages


async def _run_processor(
    entry: ProcessorEntry, record: AssetRecord, changes: set[str]
) -> tuple[ProcessorResult, bool]:
    try:
        logger.debug("Running processor {} for record {}", entry.name, record.id)
        result = await entry.instance.run(record, changes)
        return result, False
    except Exception as e:
        msg = f"Processor {entry.name} failed for record {record.id}: {e}"
        logger.exception(msg)
        return ProcessorResult(status=ProcessorStatus.ERROR, message=msg), True


async def process(
    *,
    record: AssetRecord,
    snapshot: Snapshot,
    database: Database,
    stages: Sequence[ProcessorStage],
    initial_changes: Iterable[str] | None = None,
) -> ProcessorTaskResult:
    """Run processors per stage, returning the union of all observed changes."""

    if not stages:
        return ProcessorTaskResult(changes=set(), failures=0)

    changes = set(initial_changes or [])
    failed_runs = 0
    for stage in stages:
        coros: list[tuple[ProcessorEntry, Any]] = []
        for entry in stage:
            try:
                should_run = entry.instance.should_run(record, changes, database)
            except Exception:
                logger.exception(
                    "Processor {}.should_run failed for record {}",
                    entry.name,
                    record.id,
                )
                continue
            if not should_run:
                continue
            coros.append((entry, _run_processor(entry, record, changes)))
        if not coros:
            continue
        results = await asyncio.gather(*(coro for _, coro in coros))
        stage_metadata: list[Metadata] = []
        for (entry, _), outcome in zip(coros, results):
            produced, failed = outcome
            if failed:
                failed_runs += 1
                continue
            if produced.status in (ProcessorStatus.CANCELLED, ProcessorStatus.ERROR):
                failed_runs += 1
                continue
            if produced.status == ProcessorStatus.SKIPPED:
                continue
            for meta in produced.metadata:
                if meta.provider_id is None:
                    meta.provider_id = getattr(
                        entry.instance, "provider_id", entry.provider_id
                    )
                stage_metadata.append(meta)
        if not stage_metadata:
            continue
        stage_changes = database.upsert_asset(record, stage_metadata, snapshot)
        if stage_changes:
            changes.update(stage_changes)
    return ProcessorTaskResult(changes=changes, failures=failed_runs)
