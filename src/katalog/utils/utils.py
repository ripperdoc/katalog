import importlib
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional, cast

from loguru import logger

from katalog.analyzers.base import Analyzer
from katalog.sources.base import SourcePlugin
from katalog.db import Database
from katalog.models import AssetRecord
from katalog.processors.base import Processor


PluginSection = Literal["sources", "processors", "analyzers"]
PluginType = Literal["source", "processor", "analyzer"]

SECTION_TO_TYPE: dict[PluginSection, PluginType] = {
    "sources": "source",
    "processors": "processor",
    "analyzers": "analyzer",
}
TYPE_TO_SECTION: dict[PluginType, PluginSection] = {
    plugin_type: section for section, plugin_type in SECTION_TO_TYPE.items()
}


@dataclass(slots=True)
class PluginRecord:
    provider_id: str
    provider_type: PluginType
    plugin_id: str
    title: str | None
    config: dict[str, Any]


def import_plugin_class(
    package_path: str, *, default_package: str = "katalog"
) -> type[Any]:
    """Dynamically import a plugin class, allowing for short module paths."""

    module_name, class_name = package_path.rsplit(".", 1)
    module_candidates = [module_name]
    if default_package and not module_name.startswith(f"{default_package}."):
        module_candidates.append(f"{default_package}.{module_name}")
    last_error: ModuleNotFoundError | None = None
    for candidate in module_candidates:
        try:
            module = importlib.import_module(candidate)
        except ModuleNotFoundError as exc:
            last_error = exc
            if candidate.startswith(f"{default_package}."):
                raise
            continue
        try:
            return getattr(module, class_name)
        except AttributeError as exc:
            raise ImportError(
                f"Unable to locate class '{class_name}' in module '{candidate}'"
            ) from exc
    if last_error:
        raise last_error
    raise ModuleNotFoundError(
        f"Unable to import module '{module_name}' for '{package_path}'"
    )


def import_processor_class(package_path: str) -> type[Processor]:
    return cast(type[Processor], import_plugin_class(package_path))


def import_analyzer_class(package_path: str) -> type[Analyzer]:
    return cast(type[Analyzer], import_plugin_class(package_path))


def import_source_class(package_path: str) -> type[SourcePlugin]:
    return cast(type[SourcePlugin], import_plugin_class(package_path))


def load_plugin_configs(
    *,
    database: Database,
    workspace: Path,
    config_filename: str = "katalog.toml",
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Load plugin configs by syncing katalog.toml into the providers table."""

    config_path = workspace / config_filename
    if config_path.exists():
        raw_config = _read_katalog_config(config_path)
        records = _records_from_config(raw_config)
        if not records:
            logger.info("No plugins defined in {}", config_path)
        _persist_plugin_records(database, records)
        _validate_provider_alignment(database, records, config_path)
    else:
        logger.info("Config file {} not found, relying on database only", config_path)
    return _configs_from_database(database)


def _read_katalog_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("rb") as handle:
        return tomllib.load(handle)


def _records_from_config(config: dict[str, Any]) -> list[PluginRecord]:
    records: list[PluginRecord] = []
    for section, provider_type in SECTION_TO_TYPE.items():
        entries = config.get(section, []) or []
        normalized = _normalize_section(section, provider_type, entries)
        records.extend(normalized)
    return records


def _normalize_section(
    section: PluginSection,
    provider_type: PluginType,
    entries: list[dict[str, Any]],
) -> list[PluginRecord]:
    seen_ids: set[str] = set()
    normalized: list[PluginRecord] = []
    for index, raw in enumerate(entries):
        data = dict(raw)
        class_path = data.get("class")
        if not class_path:
            raise ValueError(f"{section} entry #{index + 1} is missing a 'class'")
        PluginClass = import_plugin_class(class_path)
        plugin_id = getattr(PluginClass, "PLUGIN_ID", PluginClass.__module__)
        provider_id = data.get("provider_id") or data.get("id")
        if provider_type == "source" and not provider_id:
            raise ValueError("Each source entry must define an 'id'.")
        if not provider_id:
            name = data.get("id") or f"{PluginClass.__name__}:{index}"
            provider_id = f"{provider_type}:{name}"
        if provider_id in seen_ids:
            raise ValueError(
                f"Duplicate provider id '{provider_id}' in section '{section}'"
            )
        seen_ids.add(provider_id)
        data["provider_id"] = provider_id
        if section == "sources":
            data.setdefault("id", provider_id)
        data.setdefault("_sequence", index)
        default_title_prefix = {
            "source": "Source",
            "processor": "Processor",
            "analyzer": "Analyzer",
        }[provider_type]
        data.setdefault("title", f"{default_title_prefix} {provider_id}")
        normalized.append(
            PluginRecord(
                provider_id=provider_id,
                provider_type=provider_type,
                plugin_id=plugin_id,
                title=data.get("title"),
                config=data,
            )
        )
    return normalized


def _persist_plugin_records(database: Database, records: list[PluginRecord]) -> None:
    for record in records:
        database.ensure_source(
            record.provider_id,
            title=record.title,
            plugin_id=record.plugin_id,
            config=record.config,
            provider_type=record.provider_type,
        )


def _validate_provider_alignment(
    database: Database,
    records: list[PluginRecord],
    config_path: Path,
) -> None:
    expected: dict[PluginType, set[str]] = {
        provider_type: set() for provider_type in SECTION_TO_TYPE.values()
    }
    for record in records:
        expected[record.provider_type].add(record.provider_id)
    actual: dict[PluginType, set[str]] = {}
    for row in database.list_providers():
        provider_type = row["type"]
        if provider_type in expected:
            actual.setdefault(provider_type, set()).add(row["id"])
    for provider_type, ids in expected.items():
        actual_ids = actual.get(provider_type, set())
        missing = ids - actual_ids
        extras = actual_ids - ids
        if missing or extras:
            details = []
            if missing:
                details.append(f"missing {sorted(missing)}")
            if extras:
                details.append(f"unexpected {sorted(extras)}")
            raise RuntimeError(
                f"Unable to reconcile {config_path} with providers table for type '{provider_type}': "
                + ", ".join(details)
            )


def _configs_from_database(
    database: Database,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    sources: dict[str, dict[str, Any]] = {}
    processors: list[tuple[int, dict[str, Any]]] = []
    analyzers: list[tuple[int, dict[str, Any]]] = []
    for row in database.list_providers():
        provider_type: PluginType = row["type"]
        section = TYPE_TO_SECTION.get(provider_type)
        if not section:
            continue
        config = dict(row["config"] or {})
        config.setdefault("provider_id", row["id"])
        sequence = int(config.get("_sequence", 0))
        if section == "sources":
            config["id"] = row["id"]
            config.pop("_sequence", None)
            sources[row["id"]] = config
        elif section == "processors":
            processors.append((sequence, config))
        elif section == "analyzers":
            analyzers.append((sequence, config))
    ordered_processors = [
        cfg for _, cfg in sorted(processors, key=lambda item: item[0])
    ]
    ordered_analyzers = [cfg for _, cfg in sorted(analyzers, key=lambda item: item[0])]
    for cfg in ordered_processors + ordered_analyzers:
        cfg.pop("_sequence", None)
    return sources, ordered_processors, ordered_analyzers


def populate_accessor(record: AssetRecord, source_map: dict[str, SourcePlugin]) -> None:
    if not record or not source_map:
        return None
    source = source_map.get(record.provider_id)
    if not source:
        return None
    record.attach_accessor(source.get_accessor(record))


def timestamp_to_utc(ts: float | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.utcfromtimestamp(ts)


def parse_google_drive_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a Google Drive ISO8601 date string (e.g. '2017-10-24T15:01:04.000Z') to a Python datetime (UTC).
    Returns None if input is None or invalid.
    """
    if not dt_str:
        return None
    try:
        # Google returns ISO8601 with 'Z' for UTC
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        try:
            # Fallback: sometimes no microseconds
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return None
