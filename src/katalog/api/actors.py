from typing import Any
import tomllib

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
    config_toml: str | None = None
    disabled: bool | None = None


class ActorUpdate(BaseModel):
    name: str | None = None
    config: dict[str, Any] | None = None
    config_toml: str | None = None
    disabled: bool | None = None


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

    # Handle TOML vs config conflict
    config_toml = payload.config_toml.strip() if payload.config_toml else None
    raw_config: dict[str, Any] | None = payload.config

    if config_toml and raw_config:
        raise HTTPException(
            status_code=400,
            detail="Cannot provide both config and config_toml. Use one or the other.",
        )

    # If TOML is provided, parse and validate it
    if config_toml:
        try:
            raw_config = tomllib.loads(config_toml)
        except tomllib.TOMLDecodeError as exc:
            raise HTTPException(
                status_code=400, detail=f"Invalid TOML syntax: {exc}"
            ) from exc

    normalized_config = validate_and_normalize_config(plugin_cls, raw_config)

    actor = await Actor.create(
        name=payload.name,
        plugin_id=payload.plugin_id,
        type=spec.actor_type,
        config=normalized_config,
        config_toml=config_toml,
        disabled=bool(payload.disabled) if payload.disabled is not None else False,
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
    changesets = (
        await Changeset.filter(actor_links__actor=actor)
        .order_by("-id")
        .prefetch_related("actor_links__actor")
    )
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

    # Handle config updates with TOML conflict detection
    config_toml = payload.config_toml.strip() if payload.config_toml else None
    raw_config = payload.config

    # Check for TOML/config conflict
    if config_toml and raw_config:
        raise HTTPException(
            status_code=400,
            detail="Cannot provide both config and config_toml. Use one or the other.",
        )

    # If actor already has TOML and we're trying to update config (not TOML), reject
    if actor.config_toml and raw_config is not None and config_toml is None:
        raise HTTPException(
            status_code=400,
            detail="Actor is using TOML configuration. Clear config_toml first or provide config_toml to update.",
        )

    # Update config if provided
    if config_toml is not None or raw_config is not None:
        try:
            plugin_cls = get_plugin_class(actor.plugin_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=404, detail="Plugin not found") from exc

        # Handle TOML update (including clearing with empty string)
        if config_toml is not None:
            if config_toml == "":
                # Clear TOML mode
                actor.config_toml = None
                actor.config = {}
            else:
                # Parse and validate TOML
                try:
                    parsed_config = tomllib.loads(config_toml)
                except tomllib.TOMLDecodeError as exc:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid TOML syntax: {exc}"
                    ) from exc
                normalized = validate_and_normalize_config(plugin_cls, parsed_config)
                actor.config = normalized
                actor.config_toml = config_toml
        elif raw_config is not None:
            # Update regular config (only if no TOML)
            actor.config = validate_and_normalize_config(plugin_cls, raw_config)

    if payload.disabled is not None:
        actor.disabled = bool(payload.disabled)

    await actor.save()
    return {"actor": actor.to_dict()}
