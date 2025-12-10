from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from loguru import logger

from katalog.analyzers.base import (
    Analyzer,
    AnalyzerIssue,
    AnalyzerResult,
    FileGroupFinding,
)
from katalog.db import Database, Snapshot
from katalog.utils.utils import import_plugin_class


@dataclass(slots=True)
class AnalyzerEntry:
    """Container describing a configured analyzer instance."""

    name: str
    instance: Analyzer
    plugin_id: str
    provider_id: str
    order: int = 0


def load_analyzers(
    configs: Iterable[dict[str, Any]],
    *,
    database: Database,
) -> list[AnalyzerEntry]:
    entries = [
        _instantiate_analyzer(config, index, database)
        for index, config in enumerate(configs)
    ]
    return sorted(entries, key=lambda entry: (entry.order, entry.name))


async def run_analyzers(
    *,
    database: Database,
    analyzers: Sequence[AnalyzerEntry],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in analyzers:
        snapshot = database.begin_snapshot(
            entry.provider_id,
            metadata={"analyzer": entry.name, "plugin_id": entry.plugin_id},
        )
        try:
            try:
                should_run = entry.instance.should_run(
                    snapshot=snapshot, database=database
                )
            except Exception:
                logger.exception("Analyzer {}.should_run failed", entry.name)
                database.finalize_snapshot(snapshot, status="partial")
                results.append(
                    {
                        "analyzer": entry.name,
                        "plugin_id": entry.plugin_id,
                        "provider_id": entry.provider_id,
                        "status": "error",
                        "error": "should_run failed",
                    }
                )
                continue

            if not should_run:
                database.finalize_snapshot(snapshot, status="full")
                results.append(
                    {
                        "analyzer": entry.name,
                        "plugin_id": entry.plugin_id,
                        "provider_id": entry.provider_id,
                        "status": "skipped",
                    }
                )
                continue

            analyzer_result = await entry.instance.run(
                snapshot=snapshot, database=database
            )
            persisted = _persist_analyzer_result(
                database=database,
                snapshot=snapshot,
                entry=entry,
                result=analyzer_result,
            )
            database.finalize_snapshot(snapshot, status="full")
            results.append(persisted)
        except Exception:
            database.finalize_snapshot(snapshot, status="partial")
            logger.exception("Analyzer {} failed", entry.name)
            raise
    return results


def _instantiate_analyzer(
    config: dict[str, Any],
    index: int,
    database: Database,
) -> AnalyzerEntry:
    class_path = config.get("class")
    if not class_path:
        raise ValueError("Each analyzer config must include a 'class' field")
    AnalyzerClass = import_plugin_class(class_path)
    kwargs = {
        k: v
        for k, v in config.items()
        if k not in {"class", "id", "order", "provider_id", "title"}
    }
    instance = AnalyzerClass(**kwargs)
    name = config.get("id") or f"{AnalyzerClass.__name__}:{index}"
    provider_id = config.get("provider_id") or f"analyzer:{name}"
    plugin_id = getattr(AnalyzerClass, "PLUGIN_ID", AnalyzerClass.__module__)
    database.ensure_source(
        provider_id,
        title=config.get("title") or f"Analyzer {name}",
        plugin_id=plugin_id,
        config=config,
        provider_type="analyzer",
    )
    if not hasattr(instance, "provider_id"):
        setattr(instance, "provider_id", provider_id)
    order_value = config.get("order")
    order = int(order_value) if order_value is not None else 0
    return AnalyzerEntry(
        name=name,
        instance=instance,
        plugin_id=plugin_id,
        provider_id=provider_id,
        order=order,
    )


def _persist_analyzer_result(
    *,
    database: Database,
    snapshot: Snapshot,
    entry: AnalyzerEntry,
    result: AnalyzerResult,
) -> dict[str, Any]:
    metadata_count = 0
    if result.metadata:
        for item in result.metadata:
            if item.provider_id is None:
                item.provider_id = entry.provider_id
        metadata_count = database.insert_metadata(
            result.metadata,
            snapshot=snapshot,
            default_provider_id=entry.provider_id,
        )
    if result.relationships:
        for rel in result.relationships:
            if rel.provider_id is None:
                rel.provider_id = entry.provider_id
    relationship_count = database.replace_relationships(
        provider_id=entry.provider_id,
        snapshot=snapshot,
        relationships=result.relationships,
    )
    summary = {
        "analyzer": entry.name,
        "plugin_id": entry.plugin_id,
        "provider_id": entry.provider_id,
        "status": "completed",
        "metadata_count": metadata_count,
        "relationship_count": relationship_count,
        "group_count": len(result.groups),
        "issue_count": len(result.issues),
        "groups": [_group_to_dict(group) for group in result.groups],
        "issues": [_issue_to_dict(issue) for issue in result.issues],
    }
    return summary


def _group_to_dict(group: FileGroupFinding) -> dict[str, Any]:
    return {
        "kind": group.kind,
        "label": group.label,
        "file_ids": list(group.file_ids),
        "attributes": dict(group.attributes),
    }


def _issue_to_dict(issue: AnalyzerIssue) -> dict[str, Any]:
    return {
        "level": issue.level,
        "message": issue.message,
        "file_ids": list(issue.file_ids),
        "extra": dict(issue.extra),
    }
