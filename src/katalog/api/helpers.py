from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from katalog.plugins.registry import (
    PluginSpec,
    get_plugin_class,
    get_plugin_spec,
    refresh_plugins,
)


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
        raise HTTPException(
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
        raise HTTPException(status_code=404, detail="Plugin not found")
    try:
        plugin_cls = (
            spec.cls
            if hasattr(spec, "cls") and spec.cls
            else get_plugin_class(plugin_id)
        )
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Plugin not found") from exc
    config_model = getattr(plugin_cls, "config_model", None)
    if config_model is None:
        return {"schema": {"type": "object", "properties": {}}}
    return {"schema": config_model.model_json_schema(by_alias=False)}
