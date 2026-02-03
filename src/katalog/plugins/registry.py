from __future__ import annotations

from functools import lru_cache
from importlib.metadata import EntryPoint, entry_points
from typing import Iterable, TypeVar, cast, overload

from loguru import logger

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from katalog.models import Actor, ActorType
from katalog.db.actors import get_actor_repo
from katalog.plugins.base import PluginBase
from katalog.utils.utils import import_plugin_class


ENTRYPOINT_GROUP_BY_TYPE: dict[ActorType, str] = {
    ActorType.SOURCE: "katalog.source",
    ActorType.PROCESSOR: "katalog.processor",
    ActorType.ANALYZER: "katalog.analyzer",
    ActorType.EDITOR: "katalog.editor",
}


class PluginSpec(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    plugin_id: str
    actor_type: ActorType
    cls: type = Field(exclude=True)
    title: str
    description: str | None
    origin: str = "entrypoint"
    version: str | None = None

    @field_serializer("actor_type")
    def _serialize_actor_type(self, value: ActorType) -> str:
        return value.name if isinstance(value, ActorType) else str(value)


def _iter_entrypoints(group: str) -> Iterable[EntryPoint]:
    """Select entry points for a group using the Python 3.12 API."""

    try:
        return entry_points().select(group=group)  # type: ignore[return-value]
    except Exception:
        logger.exception("Failed to read entry points for group {group}", group=group)
        return []


def _spec_from_entrypoint(
    *, ep: EntryPoint, actor_type: ActorType
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
        actor_type=actor_type,
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
            spec = _spec_from_entrypoint(ep=ep, actor_type=ptype)
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


_InstanceT = TypeVar("_InstanceT", bound="PluginBase")
_INSTANCE_CACHE: dict[int, PluginBase] = {}


@overload
async def get_actor_instance(actor: Actor) -> PluginBase: ...


@overload
async def get_actor_instance(actor: int) -> PluginBase: ...


async def get_actor_instance(actor) -> PluginBase:
    """
    Return a cached plugin instance for the given actor (id or Actor), creating it if needed.
    """
    actor_obj: Actor | None
    if isinstance(actor, Actor):
        actor_obj = actor
    else:
        db = get_actor_repo()
        actor_obj = await db.get_or_none(id=int(actor))
    if actor_obj is None:
        raise ValueError(f"Actor {actor!r} not found")

    if actor_obj.id is None:
        raise ValueError(f"Actor {actor!r} is missing an id")
    if actor_obj.plugin_id is None:
        raise ValueError(f"Actor {actor!r} is missing a plugin_id")

    cached = _INSTANCE_CACHE.get(actor_obj.id)
    if cached is not None:
        return cached
    PluginClass = cast(type[PluginBase], get_plugin_class(actor_obj.plugin_id))
    instance = PluginClass(actor=actor_obj, **(actor_obj.config or {}))
    _INSTANCE_CACHE[actor_obj.id] = instance
    return instance


def plugins_for_type(actor_type: ActorType) -> list[PluginSpec]:
    return [spec for spec in list_plugins().values() if spec.actor_type == actor_type]
