from typing import Any
import hashlib
import json

from pydantic import ValidationError

from katalog.models.core import ActorType
from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)


class ApiError(Exception):
    """API-level error carrying HTTP-compatible fields."""

    def __init__(
        self,
        status_code: int,
        detail: Any,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.headers = headers


def validate_and_normalize_config(
    plugin_cls, config: dict[str, Any] | None
) -> dict[str, Any]:
    """Validate actor config against plugin config_model (if declared) and return normalized dict."""
    config_model = getattr(plugin_cls, "config_model", None)
    if config_model is None:
        return config or {}
    try:
        model = config_model.model_validate(config or {})
    except ValidationError as exc:
        # Use JSON-serializable error payload for REST clients.
        raise ApiError(
            status_code=400,
            detail={"message": "Invalid config", "errors": exc.errors()},
        ) from exc
    config_json = model.model_dump(mode="json", by_alias=False)
    return config_json


def config_schema_for_plugin(plugin_id: str) -> dict[str, Any]:
    spec: PluginSpec | None = get_plugin_spec(plugin_id) or refresh_plugins().get(
        plugin_id
    )
    if spec is None:
        raise ApiError(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(plugin_id)
        )
    except Exception as exc:
        raise ApiError(status_code=404, detail="Plugin not found") from exc
    config_model = getattr(plugin_cls, "config_model", None)
    if config_model is None:
        return {"schema": {"type": "object", "properties": {}}}
    return {"schema": config_model.model_json_schema(by_alias=False)}


def actor_identity_key(
    *,
    actor_type: ActorType,
    plugin_id: str | None,
    config: dict[str, Any] | None,
) -> str | None:
    """Return a stable identity key for actor deduplication."""
    if not plugin_id:
        return None
    normalized = config or {}
    config_json = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    payload = f"{int(actor_type)}|{plugin_id}|{config_json}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
