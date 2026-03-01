from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import sys


def _ensure_src_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    src = root / "src"
    if src.exists():
        sys.path.insert(0, str(src))


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a katalog WorkflowSpec that scans a JSON export where "
            "records live under a map field (default: documents)."
        )
    )
    parser.add_argument(
        "json_file",
        help="Path to JSON export file",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Workspace path (defaults to KATALOG_WORKSPACE or current directory)",
    )
    parser.add_argument(
        "--records-field",
        default="documents",
        help="Top-level field containing the document map",
    )
    parser.add_argument(
        "--url-field",
        default="uri",
        help="Field inside each document containing URL",
    )
    parser.add_argument(
        "--namespace",
        default="web",
        help="Asset namespace for emitted assets",
    )
    parser.add_argument(
        "--no-http-recursion",
        action="store_true",
        help="Skip adding HttpUrlSource to workflow",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated WorkflowSpec and exit",
    )
    parser.add_argument(
        "--verbose-http-errors",
        action="store_true",
        help="Show verbose Crawlee/impit HTTP error logs",
    )
    return parser


def _resolve_workspace(value: str | None) -> Path:
    workspace_value = value or os.environ.get("KATALOG_WORKSPACE")
    if workspace_value:
        workspace = Path(workspace_value).expanduser().resolve()
    else:
        workspace = Path.cwd().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def _validate_json_shape(json_path: Path, records_field: str) -> None:
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Expected root JSON object")
    records = payload.get(records_field)
    if not isinstance(records, dict):
        raise ValueError(
            f"Expected '{records_field}' to be an object map of external_id -> document"
        )


async def _run(args: argparse.Namespace) -> dict:
    from katalog.lifespan import app_lifespan
    from katalog.models import ActorType
    from katalog.workflows import WorkflowActorSpec, WorkflowSpec, run_workflow_file

    json_path = Path(args.json_file).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    _validate_json_shape(json_path, records_field=args.records_field)

    workspace = _resolve_workspace(args.workspace)
    workflow_id = f"json-import-{json_path.stem}"

    actors = [
        WorkflowActorSpec(
            name="JSON map documents",
            plugin_id="katalog.sources.json_list.JsonListSource",
            actor_type=ActorType.SOURCE,
            config={
                "namespace": args.namespace,
                "json_file": str(json_path),
                "records_field": args.records_field,
                "records_are_map": True,
                "url_field": args.url_field,
                "emit_record_json": False,
            },
            disabled=False,
        )
    ]
    if not args.no_http_recursion:
        actors.append(
            WorkflowActorSpec(
                name="HTTP recursive metadata",
                plugin_id="katalog.sources.http_url.HttpUrlSource",
                actor_type=ActorType.SOURCE,
                config={"verbose_crawler_logs": args.verbose_http_errors},
                disabled=False,
            )
        )

    spec = WorkflowSpec(
        file_name="in-memory-json.workflow.toml",
        file_path=f"<json:{json_path.name}>",
        workflow_id=workflow_id,
        name=f"JSON import {json_path.stem}",
        description=f"Import JSON documents from {json_path.name}",
        version="1.0.0",
        actors=actors,
    )

    if args.dry_run:
        return {
            "workspace": str(workspace),
            "workflow_spec": {
                "file_name": spec.file_name,
                "file_path": spec.file_path,
                "workflow_id": spec.workflow_id,
                "name": spec.name,
                "description": spec.description,
                "version": spec.version,
                "actors": [
                    {
                        "name": actor.name,
                        "plugin_id": actor.plugin_id,
                        "actor_type": actor.actor_type.name,
                        "disabled": actor.disabled,
                        "config": actor.config,
                    }
                    for actor in spec.actors
                ],
            },
        }

    async with app_lifespan(workspace=workspace, init_mode="full"):
        result = await run_workflow_file(spec, sync_first=True)
    return {"workspace": str(workspace), "result": result.model_dump(mode="json")}


def _configure_http_error_logging(verbose_http_errors: bool) -> None:
    if verbose_http_errors:
        return
    for logger_name in ("crawlee", "impit"):
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)


def main() -> int:
    _ensure_src_on_path()
    parser = _build_argument_parser()
    args = parser.parse_args()
    _configure_http_error_logging(args.verbose_http_errors)
    output = asyncio.run(_run(args))
    print(json.dumps(output, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
