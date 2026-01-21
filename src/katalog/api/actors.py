from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from katalog.models import Actor, Changeset
from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)

from katalog.api.helpers import config_schema_for_plugin, validate_and_normalize_config

router = APIRouter()


class ActorCreate(BaseModel):
    name: str = Field(min_length=1)
    plugin_id: str
    config: dict[str, Any] | None = None


class ActorUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None


@router.post("/actors")
async def create_actor(request: Request):
    payload = ActorCreate.model_validate(await request.json())
    spec: PluginSpec | None = get_plugin_spec(payload.plugin_id)
    if spec is None:
        spec = refresh_plugins().get(payload.plugin_id)
    if spec is None:
        raise HTTPException(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(payload.plugin_id)
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail="Plugin not found") from exc

    existing = await Actor.get_or_none(name=payload.name)
    if existing:
        raise HTTPException(status_code=400, detail="Actor name already exists")

    raw_config: dict[str, Any] | None = payload.config

    normalized_config = validate_and_normalize_config(plugin_cls, raw_config)

    actor = await Actor.create(
        name=payload.name,
        plugin_id=payload.plugin_id,
        type=spec.actor_type,
        config=normalized_config,
    )
    return {"actor": actor.to_dict()}


@router.get("/actors")
async def list_actors():
    actors = await Actor.all().order_by("id")
    return {"actors": [p.to_dict() for p in actors]}


@router.get("/actors/{actor_id}")
async def get_actor(actor_id: int):
    actor = await Actor.get_or_none(id=actor_id)
    if actor is None:
        raise HTTPException(status_code=404, detail="Actor not found")
    changesets = await Changeset.filter(actor=actor).order_by("-started_at")
    for changeset in changesets:
        await changeset.fetch_related("actor")
    return {
        "actor": actor.to_dict(),
        "changesets": [s.to_dict() for s in changesets],
    }


@router.get("/actors/{actor_id}/config/schema")
async def get_actor_config_schema(actor_id: int):
    actor = await Actor.get_or_none(id=actor_id)
    if actor is None:
        raise HTTPException(status_code=404, detail="Actor not found")
    schema_payload = config_schema_for_plugin(actor.plugin_id)
    return {**schema_payload, "value": actor.config or {}}


@router.patch("/actors/{actor_id}")
async def update_actor(actor_id: int, request: Request):
    actor = await Actor.get_or_none(id=actor_id)
    if actor is None:
        raise HTTPException(status_code=404, detail="Actor not found")
    payload = ActorUpdate.model_validate(await request.json())
    if payload.name:
        actor.name = payload.name
    if payload.config is not None:
        try:
            plugin_cls = get_plugin_class(actor.plugin_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=404, detail="Plugin not found") from exc
        actor.config = validate_and_normalize_config(plugin_cls, payload.config)
    await actor.save()
    return {"actor": actor.to_dict()}
