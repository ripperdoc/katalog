from __future__ import annotations

import inspect
from typing import Any

from loguru import logger

from katalog.api.helpers import ApiError
from katalog.db.actors import get_actor_repo
from katalog.models import Actor, ActorType
from katalog.models.views import ViewSpec, get_view, list_views
from katalog.plugins.base import PluginBase
from katalog.plugins.registry import get_actor_instance


def _normalize_view(raw: ViewSpec | dict[str, Any]) -> ViewSpec:
    if isinstance(raw, ViewSpec):
        return raw
    return ViewSpec.model_validate(raw)


async def _plugin_views_for_actor(actor: Actor) -> list[ViewSpec]:
    if actor.id is None:
        return []

    try:
        instance = await get_actor_instance(actor)
    except Exception:
        logger.exception(
            "Failed to load actor instance for runtime views actor_id={actor_id}",
            actor_id=actor.id,
        )
        return []

    if not isinstance(instance, PluginBase):
        return []

    try:
        contributed = instance.view_definitions()
        if inspect.isawaitable(contributed):
            contributed = await contributed
    except Exception:
        logger.exception(
            "Plugin view hook failed actor_id={actor_id} plugin_id={plugin_id}",
            actor_id=actor.id,
            plugin_id=actor.plugin_id,
        )
        return []

    runtime_views: list[ViewSpec] = []
    for raw_view in list(contributed or []):
        try:
            view = _normalize_view(raw_view)
        except Exception:
            logger.exception(
                "Invalid plugin view definition actor_id={actor_id} plugin_id={plugin_id}",
                actor_id=actor.id,
                plugin_id=actor.plugin_id,
            )
            continue
        local_id = str(view.id or "default").strip() or "default"
        runtime_id = f"actor:{int(actor.id)}:{local_id}"
        runtime_name = f"{actor.name} / {view.name}"
        runtime_views.append(
            view.model_copy(update={"id": runtime_id, "name": runtime_name})
        )
    return runtime_views


async def runtime_views_api() -> list[ViewSpec]:
    db = get_actor_repo()
    actors = await db.list_rows(order_by="id", type=ActorType.SOURCE, disabled=False)
    views: list[ViewSpec] = []
    for actor in actors:
        views.extend(await _plugin_views_for_actor(actor))
    return views


async def get_view_api(view_id: str) -> ViewSpec:
    """Return a view spec by id or raise API not found."""
    try:
        return get_view(view_id)
    except KeyError:
        pass

    runtime_views = await runtime_views_api()
    for view in runtime_views:
        if view.id == view_id:
            return view
    raise ApiError(status_code=404, detail="View not found")


async def list_views_api() -> list[ViewSpec]:
    """List available view specs."""
    base_views = list(list_views())
    runtime_views = await runtime_views_api()

    seen_ids = {view.id for view in base_views}
    for runtime_view in runtime_views:
        if runtime_view.id in seen_ids:
            logger.warning(
                "Skipping runtime view with duplicate id={view_id}",
                view_id=runtime_view.id,
            )
            continue
        seen_ids.add(runtime_view.id)
        base_views.append(runtime_view)
    return base_views
