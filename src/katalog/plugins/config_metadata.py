from __future__ import annotations

from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict
from loguru import logger

from katalog.constants.metadata import (
    METADATA_REGISTRY,
    MetadataDef,
    MetadataKey,
    MetadataType,
)
from katalog.db.actors import get_actor_repo
from katalog.models import Actor
from katalog.plugins.registry import get_plugin_class


class ConfigMetadataDefInput(BaseModel):
    """Input schema for config-defined metadata entries."""

    model_config = ConfigDict(extra="forbid")

    key: str
    value_type: MetadataType
    title: str = ""
    description: str = ""
    width: int | None = None
    skip_false: bool = False
    clear_on_false: bool = False
    searchable: bool | None = None


def _build_metadata_def(
    *,
    plugin_id: str,
    entry: MetadataDef | dict[str, Any],
) -> MetadataDef:
    if isinstance(entry, MetadataDef):
        key = MetadataKey(str(entry.key).strip())
        if not str(key):
            raise ValueError("Config metadata key cannot be empty")
        return entry.model_copy(
            update={
                "plugin_id": plugin_id,
                "key": key,
                "registry_id": None,
            }
        )

    parsed = ConfigMetadataDefInput.model_validate(entry)
    key = MetadataKey(parsed.key.strip())
    if not str(key):
        raise ValueError("Config metadata key cannot be empty")

    return MetadataDef(
        plugin_id=plugin_id,
        key=key,
        registry_id=None,
        value_type=parsed.value_type,
        title=parsed.title,
        description=parsed.description,
        width=parsed.width,
        skip_false=parsed.skip_false,
        clear_on_false=parsed.clear_on_false,
        searchable=parsed.searchable,
    )


def collect_config_metadata_definitions(
    *,
    plugin_cls: type,
    plugin_id: str,
    config: dict[str, Any] | None,
) -> list[MetadataDef]:
    """Collect config-defined metadata entries from a plugin class."""
    provider = getattr(plugin_cls, "metadata_definitions_from_config", None)
    if provider is None:
        return []
    raw_defs = provider(config or {})
    if not raw_defs:
        return []
    if not isinstance(raw_defs, list):
        raise ValueError("metadata_definitions_from_config must return a list")
    return [
        _build_metadata_def(plugin_id=plugin_id, entry=entry)
        for entry in raw_defs
    ]


def register_config_metadata_definitions(definitions: Sequence[MetadataDef]) -> bool:
    """Upsert config-defined metadata into the in-memory registry."""
    changed = False
    for definition in definitions:
        existing = METADATA_REGISTRY.get(definition.key)
        if existing is None:
            METADATA_REGISTRY[definition.key] = definition
            changed = True
            continue
        if existing.value_type != definition.value_type:
            raise ValueError(
                f"Metadata key '{definition.key}' already exists with a different type "
                f"({existing.value_type.name.lower()} != {definition.value_type.name.lower()})"
            )
        if existing.plugin_id != definition.plugin_id:
            # Reuse existing metadata keys across plugins when type matches.
            # This enables actor configs to map tabular fields directly to built-in
            # keys such as `file/title` without redefining ownership.
            continue
        updated = definition.model_copy(update={"registry_id": existing.registry_id})
        if updated != existing:
            METADATA_REGISTRY[definition.key] = updated
            changed = True
    return changed


def apply_actor_config_metadata_definitions(actor: Actor) -> bool:
    """Apply one actor's config-defined metadata to the in-memory registry."""
    plugin_id = actor.plugin_id
    if not plugin_id:
        return False
    plugin_cls = get_plugin_class(plugin_id)
    definitions = collect_config_metadata_definitions(
        plugin_cls=plugin_cls,
        plugin_id=plugin_id,
        config=actor.config or {},
    )
    return register_config_metadata_definitions(definitions)


async def apply_all_actor_config_metadata_definitions() -> bool:
    """Apply config-defined metadata entries for all configured actors."""
    actor_repo = get_actor_repo()
    actors = await actor_repo.list_rows(order_by="id")
    changed = False
    for actor in actors:
        if actor.plugin_id is None:
            continue
        try:
            actor_changed = apply_actor_config_metadata_definitions(actor)
        except Exception:
            # Keep startup sync resilient even when optional plugin deps are unavailable.
            logger.warning(
                "Skipping config metadata sync for actor {actor_id}:{plugin_id}",
                actor_id=actor.id,
                plugin_id=actor.plugin_id,
            )
            continue
        changed = changed or actor_changed
    return changed
