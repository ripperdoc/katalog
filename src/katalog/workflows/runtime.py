from __future__ import annotations

import pathlib
import traceback
from typing import Any

from loguru import logger

from katalog.analyzers.base import AnalyzerScope
from katalog.analyzers.runtime import do_run_analyzer
from katalog.api.actors import ActorCreate, create_actor
from katalog.api.helpers import actor_identity_key
from katalog.api.operations import run_processors, run_source
from katalog.db.actors import get_actor_repo
from katalog.db.changesets import get_changeset_repo
from katalog.models import Actor, ActorType, OpStatus
from katalog.plugins.registry import get_plugin_class
from katalog.workflows.specs import (
    WorkflowActorSpec,
    WorkflowSpec,
    parse_workflow_file,
    parse_workflow_payload,
)


def load_workflow_specs(workflow_file: pathlib.Path) -> list[WorkflowActorSpec]:
    return parse_workflow_file(workflow_file).actors


def load_workflow_spec(workflow_file: pathlib.Path) -> WorkflowSpec:
    return parse_workflow_file(workflow_file)


def discover_workflow_files(workspace: pathlib.Path) -> list[pathlib.Path]:
    files = sorted(
        {
            *workspace.glob("*.workflow.toml"),
            *workspace.glob("workflow*.toml"),
        }
    )
    return [path.resolve() for path in files if path.is_file()]


def _compute_processor_stages(spec: WorkflowSpec) -> list[list[str]]:
    processor_specs = [
        actor for actor in spec.actors if actor.actor_type == ActorType.PROCESSOR
    ]
    if not processor_specs:
        return []

    order_by_name: dict[str, int] = {}
    dependencies_by_name: dict[str, set] = {}
    outputs_by_name: dict[str, set] = {}
    for index, actor_spec in enumerate(processor_specs):
        order_by_name[actor_spec.name] = index
        actor = Actor(
            id=None,
            name=actor_spec.name,
            plugin_id=actor_spec.plugin_id,
            type=actor_spec.actor_type,
            config=actor_spec.config,
            disabled=actor_spec.disabled,
        )
        processor_cls = get_plugin_class(actor_spec.plugin_id)
        processor = processor_cls(actor=actor, **(actor_spec.config or {}))
        dependencies_by_name[actor_spec.name] = set(processor.dependencies)
        outputs_by_name[actor_spec.name] = set(processor.outputs)

    field_to_producers: dict[Any, set[str]] = {}
    for name, outputs in outputs_by_name.items():
        for output in outputs:
            field_to_producers.setdefault(output, set()).add(name)

    remaining: dict[str, set[str]] = {}
    for name, deps in dependencies_by_name.items():
        producers: set[str] = set()
        for dependency in deps:
            producers.update(field_to_producers.get(dependency, set()))
        producers.discard(name)
        remaining[name] = producers

    stages: list[list[str]] = []
    while remaining:
        ready = sorted(
            [name for name, deps in remaining.items() if not deps],
            key=lambda n: order_by_name[n],
        )
        if not ready:
            raise ValueError(
                f"{spec.file_name}: circular processor dependencies in workflow"
            )
        stages.append(ready)
        for name in ready:
            remaining.pop(name, None)
        for deps in remaining.values():
            deps.difference_update(ready)
    return stages


async def _sync_workflow_actor_specs(
    specs: list[WorkflowActorSpec], *, workflow_label: str
) -> list[Actor]:
    synced: list[Actor] = []
    db = get_actor_repo()
    for spec in specs:
        actor = await create_actor(
            ActorCreate(
                name=spec.name,
                plugin_id=spec.plugin_id,
                config=spec.config or None,
                disabled=spec.disabled,
            )
        )
        changed = False
        if actor.name != spec.name:
            actor.name = spec.name
            changed = True
        if actor.disabled != spec.disabled:
            actor.disabled = spec.disabled
            changed = True
        if changed:
            await db.save(actor)
        synced.append(actor)

    logger.info(
        "Loaded workflow file={file} actors={count}",
        file=workflow_label,
        count=len(synced),
    )
    return synced


def _coerce_workflow_spec(workflow: pathlib.Path | WorkflowSpec) -> WorkflowSpec:
    if isinstance(workflow, WorkflowSpec):
        payload = {
            "workflow": {
                "id": workflow.workflow_id,
                "name": workflow.name,
                "description": workflow.description,
                "version": workflow.version,
            },
            "actors": [
                {
                    "name": actor.name,
                    "plugin_id": actor.plugin_id,
                    "disabled": actor.disabled,
                    "config": actor.config or {},
                }
                for actor in workflow.actors
            ],
        }
        return parse_workflow_payload(
            payload,
            file_name=workflow.file_name,
            file_path=workflow.file_path,
            fallback_name=workflow.name or "in-memory-workflow",
        )
    return load_workflow_spec(workflow)


async def sync_workflow_file(workflow_file: pathlib.Path | WorkflowSpec) -> list[Actor]:
    spec = _coerce_workflow_spec(workflow_file)
    return await _sync_workflow_actor_specs(spec.actors, workflow_label=spec.file_path)


async def _resolve_workflow_actors(
    *,
    specs: list[WorkflowActorSpec],
    workflow_label: str,
) -> list[Actor]:
    db = get_actor_repo()
    resolved: list[Actor] = []
    for spec in specs:
        identity = actor_identity_key(
            actor_type=spec.actor_type,
            plugin_id=spec.plugin_id,
            config=spec.config or {},
        )
        if identity is None:
            raise ValueError(f"Could not compute identity for actor '{spec.name}'")
        actor = await db.get_or_none(type=spec.actor_type, identity_key=identity)
        if actor is None:
            raise ValueError(
                f"Workflow actor '{spec.name}' ({spec.plugin_id}) is missing from DB for '{workflow_label}'. Run workflow sync first."
            )
        resolved.append(actor)
    return resolved


async def _run_workflow_analyzers(analyzers: list[Actor]) -> list[int]:
    if not analyzers:
        return []
    changeset_db = get_changeset_repo()
    analyzer_changesets: list[int] = []
    for analyzer in analyzers:
        if analyzer.id is None:
            continue
        changeset = await changeset_db.begin(
            message=f"Analyzer run: {analyzer.name}",
            actors=[analyzer],
            status=OpStatus.IN_PROGRESS,
        )
        try:
            await do_run_analyzer(
                analyzer,
                changeset=changeset,
                scope=AnalyzerScope.all(),
            )
            await changeset.finalize(status=OpStatus.COMPLETED)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Workflow analyzer failed analyzer_id={analyzer_id}",
                analyzer_id=analyzer.id,
            )
            data = dict(changeset.data or {})
            data["error_message"] = str(exc)
            data["error_traceback"] = traceback.format_exc()
            changeset.data = data
            await changeset.finalize(status=OpStatus.ERROR)
            raise
        analyzer_changesets.append(int(changeset.id))
    return analyzer_changesets


async def run_workflow_file(
    workflow_file: pathlib.Path | WorkflowSpec, *, sync_first: bool = False
) -> dict:
    spec = _coerce_workflow_spec(workflow_file)
    if sync_first:
        actors = await sync_workflow_file(spec)
    else:
        actors = await _resolve_workflow_actors(
            specs=spec.actors,
            workflow_label=spec.file_path,
        )

    source_actors = [a for a in actors if a.type == ActorType.SOURCE and not a.disabled]
    processor_actors = [
        a for a in actors if a.type == ActorType.PROCESSOR and not a.disabled
    ]
    analyzer_actors = [
        a for a in actors if a.type == ActorType.ANALYZER and not a.disabled
    ]

    source_changesets: list[int] = []
    processor_changeset: int | None = None
    analyzer_changesets: list[int] = []
    for source in source_actors:
        if source.id is None:
            continue
        changeset = await run_source(
            int(source.id),
            finalize=True,
            run_processors=False,
        )
        source_changesets.append(int(changeset.id))

    if processor_actors:
        processor_ids = [
            int(actor.id) for actor in processor_actors if actor.id is not None
        ]
        if processor_ids:
            changeset = await run_processors(
                processor_ids=processor_ids,
                asset_ids=None,
                finalize=True,
            )
            processor_changeset = int(changeset.id)

    if analyzer_actors:
        analyzer_changesets = await _run_workflow_analyzers(analyzer_actors)

    return {
        "workflow_file": spec.file_path,
        "actors": len(actors),
        "sources_run": len(source_changesets),
        "processors_run": len(processor_actors),
        "analyzers_run": len(analyzer_changesets),
        "source_changesets": source_changesets,
        "processor_changeset": processor_changeset,
        "analyzer_changesets": analyzer_changesets,
        "last_changeset_id": (
            analyzer_changesets[-1]
            if analyzer_changesets
            else (
                processor_changeset
                if processor_changeset is not None
                else (source_changesets[-1] if source_changesets else None)
            )
        ),
    }


async def start_workflow_file(
    workflow_file: pathlib.Path | WorkflowSpec, *, sync_first: bool = False
) -> dict[str, Any]:
    spec = _coerce_workflow_spec(workflow_file)
    if sync_first:
        actors = await sync_workflow_file(spec)
    else:
        actors = await _resolve_workflow_actors(
            specs=spec.actors,
            workflow_label=spec.file_path,
        )

    source_actors = [a for a in actors if a.type == ActorType.SOURCE and not a.disabled]
    processor_actors = [
        a for a in actors if a.type == ActorType.PROCESSOR and not a.disabled
    ]
    analyzer_actors = [
        a for a in actors if a.type == ActorType.ANALYZER and not a.disabled
    ]

    source_changesets: list[int] = []
    for source in source_actors:
        if source.id is None:
            continue
        changeset = await run_source(
            int(source.id),
            finalize=True,
            run_processors=False,
        )
        source_changesets.append(int(changeset.id))

    processor_changeset = None
    if processor_actors:
        processor_ids = [
            int(actor.id) for actor in processor_actors if actor.id is not None
        ]
        if processor_ids:
            started = await run_processors(
                processor_ids=processor_ids,
                asset_ids=None,
                finalize=False,
            )
            processor_changeset = started

    return {
        "workflow_file": spec.file_path,
        "actors": len(actors),
        "sources_run": len(source_changesets),
        "processors_run": len(processor_actors),
        "analyzers_run": 0,
        "source_changesets": source_changesets,
        "processor_changeset": (
            int(processor_changeset.id) if processor_changeset is not None else None
        ),
        "analyzer_changesets": [],
        "last_changeset_id": (
            int(processor_changeset.id)
            if processor_changeset is not None
            else (source_changesets[-1] if source_changesets else None)
        ),
        "changeset": processor_changeset,
    }


async def workflow_status(workflow_file: pathlib.Path | WorkflowSpec) -> dict[str, Any]:
    spec = _coerce_workflow_spec(workflow_file)
    db = get_actor_repo()
    resolved = 0
    total = len(spec.actors)
    for actor in spec.actors:
        identity = actor_identity_key(
            actor_type=actor.actor_type,
            plugin_id=actor.plugin_id,
            config=actor.config or {},
        )
        if identity is None:
            continue
        existing = await db.get_or_none(type=actor.actor_type, identity_key=identity)
        if existing is not None:
            resolved += 1

    source_count = sum(
        1 for actor in spec.actors if actor.actor_type == ActorType.SOURCE
    )
    processor_count = sum(
        1 for actor in spec.actors if actor.actor_type == ActorType.PROCESSOR
    )
    analyzer_count = sum(
        1 for actor in spec.actors if actor.actor_type == ActorType.ANALYZER
    )
    status = "ready" if total == resolved else "not-synced"
    actor_names = [actor.name for actor in spec.actors]
    processor_stages = _compute_processor_stages(spec)
    return {
        "file_name": spec.file_name,
        "file_path": spec.file_path,
        "workflow_id": spec.workflow_id,
        "name": spec.name,
        "description": spec.description,
        "version": spec.version,
        "actor_count": total,
        "source_count": source_count,
        "processor_count": processor_count,
        "analyzer_count": analyzer_count,
        "resolved_actor_count": resolved,
        "status": status,
        "actor_names": actor_names,
        "processor_stages": processor_stages,
    }
