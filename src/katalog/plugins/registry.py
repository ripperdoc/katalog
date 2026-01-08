from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from importlib.metadata import EntryPoint, entry_points
from typing import Iterable

from loguru import logger

from katalog.models import ProviderType
from katalog.utils.utils import import_plugin_class


ENTRYPOINT_GROUP_BY_TYPE: dict[ProviderType, str] = {
    ProviderType.SOURCE: "katalog.source",
    ProviderType.PROCESSOR: "katalog.processor",
    ProviderType.ANALYZER: "katalog.analyzer",
}


@dataclass(slots=True)
class PluginSpec:
    plugin_id: str
    provider_type: ProviderType
    cls: type
    title: str
    description: str | None
    origin: str = "entrypoint"
    version: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "plugin_id": self.plugin_id,
            "type": self.provider_type.name,
            "title": self.title,
            "description": self.description,
            "origin": self.origin,
            "version": self.version,
        }


def _iter_entrypoints(group: str) -> Iterable[EntryPoint]:
    """Select entry points for a group using the Python 3.12 API."""

    try:
        return entry_points().select(group=group)  # type: ignore[return-value]
    except Exception:
        logger.exception("Failed to read entry points for group {group}", group=group)
        return []


def _spec_from_entrypoint(
    *, ep: EntryPoint, provider_type: ProviderType
) -> PluginSpec | None:
    try:
        cls = ep.load()
    except Exception:
        logger.exception("Failed to load plugin entry point {name}", name=ep.name)
        return None

    plugin_id = getattr(cls, "plugin_id", ep.name) or ep.name
    title = getattr(cls, "title", None) or getattr(cls, "__name__", ep.name)
    description = getattr(cls, "description", None) or getattr(cls, "__doc__", None)
    version = getattr(cls, "version", None) or getattr(cls, "__version__", None)

    return PluginSpec(
        plugin_id=plugin_id,
        provider_type=provider_type,
        cls=cls,
        title=str(title),
        description=str(description).strip() if description else None,
        origin="entrypoint",
        version=str(version) if version else None,
    )


@lru_cache(maxsize=1)
def list_plugins() -> dict[str, PluginSpec]:
    """Discover plugins exposed via entry points. Cached until explicitly refreshed."""

    discovered: dict[str, PluginSpec] = {}
    for ptype, group in ENTRYPOINT_GROUP_BY_TYPE.items():
        for ep in _iter_entrypoints(group):
            spec = _spec_from_entrypoint(ep=ep, provider_type=ptype)
            if spec is None:
                continue
            if spec.plugin_id in discovered:
                logger.warning(
                    "Plugin id {plugin_id} declared multiple times; keeping first",
                    plugin_id=spec.plugin_id,
                )
                continue
            discovered[spec.plugin_id] = spec
    return discovered


def refresh_plugins() -> dict[str, PluginSpec]:
    """Clear cache and re-discover plugins."""

    list_plugins.cache_clear()
    return list_plugins()


def get_plugin_spec(plugin_id: str) -> PluginSpec | None:
    return list_plugins().get(plugin_id)


def get_plugin_class(plugin_id: str) -> type:
    """Resolve a plugin class from registry or fall back to import by path."""

    spec = get_plugin_spec(plugin_id)
    if spec:
        return spec.cls
    # Fallback for legacy/local usage: import by dotted path
    return import_plugin_class(plugin_id)


def plugins_for_type(provider_type: ProviderType) -> list[PluginSpec]:
    return [spec for spec in list_plugins().values() if spec.provider_type == provider_type]
