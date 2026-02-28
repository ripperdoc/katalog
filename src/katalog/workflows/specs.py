from __future__ import annotations

from dataclasses import dataclass
import pathlib
import tomllib
from typing import Mapping

from katalog.api.helpers import validate_and_normalize_config
from katalog.models import ActorType
from katalog.plugins.registry import get_plugin_class, get_plugin_spec, refresh_plugins


@dataclass(frozen=True)
class WorkflowActorSpec:
    name: str
    plugin_id: str
    actor_type: ActorType
    config: dict
    disabled: bool


@dataclass(frozen=True)
class WorkflowSpec:
    file_name: str
    file_path: str
    workflow_id: str | None
    name: str
    description: str | None
    version: str | None
    actors: list[WorkflowActorSpec]


def parse_workflow_file(workflow_file: pathlib.Path) -> WorkflowSpec:
    if not workflow_file.exists():
        raise FileNotFoundError(f"Workflow file not found: {workflow_file}")
    try:
        raw = tomllib.loads(workflow_file.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"{workflow_file.name}: invalid TOML syntax: {exc}") from exc
    return parse_workflow_payload(
        raw,
        file_name=workflow_file.name,
        file_path=str(workflow_file),
        fallback_name=workflow_file.stem,
    )


def parse_workflow_payload(
    raw: Mapping[str, Any],
    *,
    file_name: str,
    file_path: str,
    fallback_name: str,
) -> WorkflowSpec:
    workflow_block = raw.get("workflow") or {}
    if workflow_block and not isinstance(workflow_block, dict):
        raise ValueError(f"{file_name}: 'workflow' must be a table")

    entries = raw.get("actors") or []
    if not isinstance(entries, list):
        raise ValueError(f"{file_name}: 'actors' must be a list")

    plugins = refresh_plugins()
    actor_specs: list[WorkflowActorSpec] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{file_name}: actor #{index + 1} must be a table")
        plugin_id = entry.get("plugin_id")
        if not plugin_id or not isinstance(plugin_id, str):
            raise ValueError(f"{file_name}: actor #{index + 1} is missing 'plugin_id'")
        plugin_spec = get_plugin_spec(plugin_id) or plugins.get(plugin_id)
        if plugin_spec is None:
            raise ValueError(
                f"{file_name}: actor #{index + 1} references unknown plugin '{plugin_id}'"
            )
        name = str(entry.get("name") or plugin_id)
        disabled = bool(entry.get("disabled")) if "disabled" in entry else False
        if "config" in entry:
            config = entry.get("config") or {}
            if not isinstance(config, dict):
                raise ValueError(
                    f"{file_name}: actor #{index + 1} config must be a table"
                )
        else:
            reserved = {"name", "plugin_id", "disabled"}
            config = {k: v for k, v in entry.items() if k not in reserved}

        try:
            plugin_cls = (
                plugin_spec.cls
                if hasattr(plugin_spec, "cls") and plugin_spec.cls
                else get_plugin_class(plugin_id)
            )
            config = validate_and_normalize_config(plugin_cls, config)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                f"{file_name}: actor #{index + 1} has invalid config: {exc}"
            ) from exc

        actor_specs.append(
            WorkflowActorSpec(
                name=name,
                plugin_id=plugin_id,
                actor_type=plugin_spec.actor_type,
                config=config,
                disabled=disabled,
            )
        )

    return WorkflowSpec(
        file_name=file_name,
        file_path=file_path,
        workflow_id=workflow_block.get("id")
        if isinstance(workflow_block.get("id"), str)
        else None,
        name=(
            workflow_block.get("name")
            if isinstance(workflow_block.get("name"), str)
            else fallback_name
        ),
        description=(
            workflow_block.get("description")
            if isinstance(workflow_block.get("description"), str)
            else None
        ),
        version=(
            workflow_block.get("version")
            if isinstance(workflow_block.get("version"), str)
            else None
        ),
        actors=actor_specs,
    )
