from __future__ import annotations

from typing import Any

from katalog.constants.metadata import (
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataDef,
    MetadataKey,
    MetadataType,
    metadata_registry_for_current_db,
    set_metadata_registry_cache,
)
from katalog.models import OpStatus


def dump_registry() -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for definition in metadata_registry_for_current_db().values():
        payload.append(
            {
                "plugin_id": definition.plugin_id,
                "key": str(definition.key),
                "registry_id": definition.registry_id,
                "value_type": int(definition.value_type),
                "title": definition.title,
                "description": definition.description,
                "width": definition.width,
                "skip_false": definition.skip_false,
                "clear_on_false": definition.clear_on_false,
                "searchable": definition.searchable,
            }
        )
    return payload


def seed_registry(payload: list[dict[str, Any]]) -> None:
    METADATA_REGISTRY.clear()
    METADATA_REGISTRY_BY_ID.clear()
    key_to_id: dict[MetadataKey, int] = {}
    for item in payload:
        key = MetadataKey(item["key"])
        value_type = MetadataType(int(item["value_type"]))
        definition = MetadataDef(
            plugin_id=item["plugin_id"],
            key=key,
            registry_id=item.get("registry_id"),
            value_type=value_type,
            title=item.get("title") or "",
            description=item.get("description") or "",
            width=item.get("width"),
            skip_false=bool(item.get("skip_false")),
            clear_on_false=bool(item.get("clear_on_false")),
            searchable=item.get("searchable"),
        )
        METADATA_REGISTRY[definition.key] = definition
        if definition.registry_id is not None:
            registry_id = int(definition.registry_id)
            METADATA_REGISTRY_BY_ID[registry_id] = definition
            key_to_id[definition.key] = registry_id

    set_metadata_registry_cache(
        key_to_id=key_to_id,
        defs_by_id=METADATA_REGISTRY_BY_ID,
    )


def normalize_metadata_changes_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return payload
    normalized = dict(payload)
    for key in ("loaded", "staged"):
        entries = normalized.get(key)
        if not entries:
            continue
        normalized_entries = []
        for entry in entries:
            if not isinstance(entry, dict):
                normalized_entries.append(entry)
                continue
            entry_value_type = entry.get("value_type")
            if isinstance(entry_value_type, str):
                try:
                    entry = dict(entry)
                    entry["value_type"] = MetadataType[entry_value_type]
                except KeyError:
                    pass
            normalized_entries.append(entry)
        normalized[key] = normalized_entries
    return normalized


def normalize_processor_result_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return payload
    normalized = dict(payload)
    status = normalized.get("status")
    if isinstance(status, str):
        try:
            normalized["status"] = OpStatus[status]
        except KeyError:
            pass
    metadata_entries = normalized.get("metadata")
    if metadata_entries:
        normalized_entries = []
        for entry in metadata_entries:
            if not isinstance(entry, dict):
                normalized_entries.append(entry)
                continue
            entry_value_type = entry.get("value_type")
            if isinstance(entry_value_type, str):
                try:
                    entry = dict(entry)
                    entry["value_type"] = MetadataType[entry_value_type]
                except KeyError:
                    pass
            normalized_entries.append(entry)
        normalized["metadata"] = normalized_entries
    return normalized
