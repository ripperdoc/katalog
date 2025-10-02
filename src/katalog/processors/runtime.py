from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from loguru import logger

from katalog.db import Database, Snapshot
from katalog.models import FileRecord, Metadata, MetadataKey
from katalog.processors.base import Processor
from katalog.utils.utils import import_processor_class


@dataclass(slots=True)
class ProcessorEntry:
    """Runtime wrapper that keeps processor metadata handy."""

    name: str
    instance: Processor
    dependencies: frozenset[MetadataKey]
    outputs: frozenset[MetadataKey]
    order: int


ProcessorStage = list[ProcessorEntry]


def _instantiate_processor(
    config: dict[str, Any], index: int, database: Database | None
) -> ProcessorEntry:
    class_path = config.get("class")
    if not class_path:
        raise ValueError("Each processor config must include a 'class' field")
    ProcessorClass = import_processor_class(class_path)
    kwargs = {k: v for k, v in config.items() if k not in {"class", "id", "order"}}
    if database is not None:
        kwargs.setdefault("database", database)
    instance = ProcessorClass(**kwargs)
    name = config.get("id") or f"{ProcessorClass.__name__}:{index}"
    order_value = config.get("order")
    order = int(order_value) if order_value is not None else 0
    return ProcessorEntry(
        name=name,
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
    entry: ProcessorEntry, record: FileRecord, changes: set[str]
) -> list[Metadata]:
    try:
        logger.debug("Running processor {} for record {}", entry.name, record.id)
        return await entry.instance.run(record, changes)
    except Exception:
        logger.exception("Processor {} failed for record {}", entry.name, record.id)
        return []


async def process(
    *,
    record: FileRecord,
    snapshot: Snapshot,
    database: Database,
    stages: Sequence[ProcessorStage],
    initial_changes: Iterable[str] | None = None,
) -> set[str]:
    """Run processors per stage, returning the union of all observed changes."""

    if not stages:
        return set()

    changes = set(initial_changes or [])
    for stage in stages:
        coros = []
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
            coros.append(_run_processor(entry, record, changes))
        if not coros:
            continue
        results = await asyncio.gather(*coros)
        stage_metadata: list[Metadata] = []
        for produced in results:
            if produced:
                stage_metadata.extend(produced)
        if not stage_metadata:
            continue
        stage_changes = database.upsert_file_record(record, stage_metadata, snapshot)
        if stage_changes:
            changes.update(stage_changes)
    return changes
