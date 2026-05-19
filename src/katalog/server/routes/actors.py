import asyncio
from typing import Any, cast

from fastapi import APIRouter, Request

from katalog.api.actors import (
    ActorCreate,
    ActorUpdate,
    create_actor,
    get_actor,
    get_actor_config_schema,
    list_actors,
    update_actor,
)
from katalog.models import Actor, ActorType
from katalog.plugins.registry import get_actor_instance
from katalog.sources.base import SourcePlugin

router = APIRouter()


async def _source_readiness(actor: Actor) -> dict[str, Any]:
    actor_payload = actor.model_dump(mode="json")
    if actor.type != ActorType.SOURCE:
        return actor_payload

    try:
        actor_plugin = cast(SourcePlugin, await get_actor_instance(actor))
        ready, reason = await actor_plugin.is_ready()
        actor_payload["ready"] = bool(ready)
        actor_payload["ready_reason"] = None if ready else (reason or "Source is not ready")
        return actor_payload
    except Exception as exc:  # noqa: BLE001
        actor_payload["ready"] = False
        actor_payload["ready_reason"] = f"Source is not ready: failed to load plugin instance ({exc})"
        return actor_payload


@router.post("/actors")
async def create_actor_rest(request: Request):
    payload = ActorCreate.model_validate(await request.json())
    actor = await create_actor(payload)
    return {"actor": actor}


@router.get("/actors")
async def list_actors_rest():
    actors = await list_actors()
    enriched = await asyncio.gather(*(_source_readiness(actor) for actor in actors))
    return {"actors": enriched}


@router.get("/actors/{actor_id}")
async def get_actor_rest(actor_id: int):
    actor, changesets = await get_actor(actor_id)
    actor_payload = await _source_readiness(actor)
    return {"actor": actor_payload, "changesets": changesets}


@router.get("/actors/{actor_id}/config/schema")
async def get_actor_config_schema_rest(actor_id: int):
    return await get_actor_config_schema(actor_id)


@router.patch("/actors/{actor_id}")
async def update_actor_rest(actor_id: int, request: Request):
    payload = ActorUpdate.model_validate(await request.json())
    actor = await update_actor(actor_id, payload)
    return {"actor": actor}
