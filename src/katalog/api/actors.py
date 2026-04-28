from typing import Any
import tomllib

from pydantic import BaseModel, Field

from katalog.models import Actor, Changeset
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)
from katalog.plugins.config_metadata import (
    collect_config_metadata_definitions,
    register_config_metadata_definitions,
)
from katalog.db.sqlspec.query_metadata_registry import sync_metadata_registry

from katalog.api.helpers import (
    ApiError,
    actor_identity_key,
    config_schema_for_plugin,
    requires_write_access,
    validate_and_normalize_config,
)


class ActorCreate(BaseModel):
    """Payload for creating an actor."""
    name: str = Field(min_length=1)
    plugin_id: str
    identity_key: str | None = None
    config: dict[str, Any] | None = None
    config_toml: str | None = None
    disabled: bool | None = None


class ActorUpdate(BaseModel):
    """Payload for updating an actor."""
    name: str | None = None
    identity_key: str | None = None
    config: dict[str, Any] | None = None
    config_toml: str | None = None
    disabled: bool | None = None


@requires_write_access()
async def create_actor(payload: ActorCreate) -> Actor:
    """Create an actor from validated plugin configuration."""
    db = get_actor_repo()
    spec: PluginSpec | None = get_plugin_spec(payload.plugin_id)
    if spec is None:
        spec = refresh_plugins().get(payload.plugin_id)
    if spec is None:
        raise ApiError(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(payload.plugin_id)
        )
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=404, detail="Plugin not found") from exc

    # Handle TOML vs config conflict
    config_toml = payload.config_toml.strip() if payload.config_toml else None
    raw_config: dict[str, Any] | None = payload.config

    if config_toml and raw_config:
        raise ApiError(
            status_code=400,
            detail="Cannot provide both config and config_toml. Use one or the other.",
        )

    # If TOML is provided, parse and validate it
    if config_toml:
        try:
            raw_config = tomllib.loads(config_toml)
        except tomllib.TOMLDecodeError as exc:
            raise ApiError(
                status_code=400, detail=f"Invalid TOML syntax: {exc}"
            ) from exc

    normalized_config = validate_and_normalize_config(plugin_cls, raw_config)
    try:
        definitions = collect_config_metadata_definitions(
            plugin_cls=plugin_cls,
            plugin_id=payload.plugin_id,
            config=normalized_config,
        )
        metadata_defs_changed = register_config_metadata_definitions(definitions)
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    identity_key = actor_identity_key(
        actor_type=spec.actor_type,
        plugin_id=payload.plugin_id,
        identity_key=payload.identity_key,
    )

    if identity_key is not None:
        existing = await db.get_or_none(identity_key=identity_key)
        if existing is not None:
            raise ApiError(
                status_code=400,
                detail=(
                    f"identity_key '{identity_key}' is already used by actor id={existing.id} "
                    f"({existing.type.name}:{existing.plugin_id}). Choose another identity_key."
                ),
            )

    actor = await db.create(
        name=payload.name,
        plugin_id=payload.plugin_id,
        identity_key=identity_key,
        type=spec.actor_type,
        config=normalized_config,
        config_toml=config_toml,
        disabled=bool(payload.disabled) if payload.disabled is not None else False,
    )
    if metadata_defs_changed:
        await sync_metadata_registry()
    return actor


async def list_actors() -> list[Actor]:
    """List all configured actors ordered by id."""
    db = get_actor_repo()
    actors = await db.list_rows(order_by="id")
    return actors


async def get_actor(actor_id: int) -> tuple[Actor, list[Changeset]]:
    """Return an actor together with its related changesets."""
    db = get_actor_repo()
    actor = await db.get_or_none(id=actor_id)
    if actor is None or actor.id is None:
        raise ApiError(status_code=404, detail="Actor not found")
    changeset_db = get_changeset_repo()
    changesets = await changeset_db.list_for_actor(int(actor.id))
    for changeset in changesets:
        await changeset_db.load_actor_ids(changeset)
    return actor, changesets


async def get_actor_config_schema(actor_id: int) -> dict[str, Any]:
    """Return config schema and current values for an actor plugin."""
    db = get_actor_repo()
    actor = await db.get_or_none(id=actor_id)
    if actor is None or actor.id is None:
        raise ApiError(status_code=404, detail="Actor not found")
    if actor.plugin_id is None:
        raise ApiError(status_code=409, detail="Actor is missing plugin_id")
    try:
        schema_payload = config_schema_for_plugin(actor.plugin_id)
        return {"schema": schema_payload["schema"], "value": actor.config or {}}
    except ApiError as exc:
        # In read-only/light installs, legacy actors may reference plugins whose
        # optional dependencies are not installed. Keep actor inspection usable.
        if exc.status_code == 404:
            return {
                "schema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": True,
                },
                "value": actor.config or {},
                "plugin_unavailable": True,
            }
        raise


@requires_write_access()
async def update_actor(actor_id: int, payload: ActorUpdate) -> Actor:
    """Update actor metadata and configuration."""
    db = get_actor_repo()
    actor = await db.get_or_none(id=actor_id)
    if actor is None:
        raise ApiError(status_code=404, detail="Actor not found")

    if payload.name:
        actor.name = payload.name

    # Handle config updates with TOML conflict detection
    config_toml = payload.config_toml.strip() if payload.config_toml else None
    raw_config = payload.config

    # Check for TOML/config conflict
    if config_toml and raw_config:
        raise ApiError(
            status_code=400,
            detail="Cannot provide both config and config_toml. Use one or the other.",
        )

    # If actor already has TOML and we're trying to update config (not TOML), reject
    if actor.config_toml and raw_config is not None and config_toml is None:
        raise ApiError(
            status_code=400,
            detail="Actor is using TOML configuration. Clear config_toml first or provide config_toml to update.",
        )

    plugin_cls_for_defs = None
    # Update config if provided
    if config_toml is not None or raw_config is not None:
        if actor.plugin_id is None:
            raise ApiError(status_code=409, detail="Actor is missing plugin_id")
        try:
            plugin_cls = get_plugin_class(actor.plugin_id)
        except Exception as exc:  # noqa: BLE001
            raise ApiError(status_code=404, detail="Plugin not found") from exc
        plugin_cls_for_defs = plugin_cls

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
                    raise ApiError(
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

    if payload.identity_key is not None:
        resolved_identity_key = actor_identity_key(
            actor_type=actor.type,
            plugin_id=actor.plugin_id,
            identity_key=payload.identity_key,
        )
        if resolved_identity_key is None:
            raise ApiError(
                status_code=400,
                detail="identity_key resolved to empty value",
            )
        actor.identity_key = resolved_identity_key

    identity_key = actor_identity_key(
        actor_type=actor.type,
        plugin_id=actor.plugin_id,
        identity_key=actor.identity_key,
    )
    if identity_key is not None:
        duplicate = await db.get_or_none(identity_key=identity_key)
        if duplicate is not None and duplicate.id != actor.id:
            raise ApiError(
                status_code=400,
                detail=(
                    f"identity_key '{identity_key}' is already used by actor id={duplicate.id} "
                    f"({duplicate.type.name}:{duplicate.plugin_id}). Choose another identity_key."
                ),
            )
    actor.identity_key = identity_key

    metadata_defs_changed = False
    if plugin_cls_for_defs is not None and actor.plugin_id is not None:
        try:
            definitions = collect_config_metadata_definitions(
                plugin_cls=plugin_cls_for_defs,
                plugin_id=actor.plugin_id,
                config=actor.config or {},
            )
            metadata_defs_changed = register_config_metadata_definitions(definitions)
        except ValueError as exc:
            raise ApiError(status_code=400, detail=str(exc)) from exc

    await db.save(actor)
    if metadata_defs_changed:
        await sync_metadata_registry()
    return actor
