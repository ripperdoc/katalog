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

router = APIRouter()


@router.post("/actors")
async def create_actor_rest(request: Request):
    payload = ActorCreate.model_validate(await request.json())
    actor = await create_actor(payload)
    return {"actor": actor}


@router.get("/actors")
async def list_actors_rest():
    actors = await list_actors()
    return {"actors": actors}


@router.get("/actors/{actor_id}")
async def get_actor_rest(actor_id: int):
    actor, changesets = await get_actor(actor_id)
    return {"actor": actor, "changesets": changesets}


@router.get("/actors/{actor_id}/config/schema")
async def get_actor_config_schema_rest(actor_id: int):
    return await get_actor_config_schema(actor_id)


@router.patch("/actors/{actor_id}")
async def update_actor_rest(actor_id: int, request: Request):
    payload = ActorUpdate.model_validate(await request.json())
    actor = await update_actor(actor_id, payload)
    return {"actor": actor}
