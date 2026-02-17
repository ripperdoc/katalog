import json
import re
from typing import Any

import typer

from katalog.constants.metadata import DOC_CHUNK_TEXT
from katalog.models.query import AssetQuery

from . import metadata_app
from .utils import render_table, run_cli, wants_json


def _text_preview(value: str, *, max_len: int = 90) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


@metadata_app.command("list")
def list_metadata(
    ctx: typer.Context,
    query: str | None = typer.Option(
        None, "--query", "-q", help="Search text (required for semantic/hybrid mode)."
    ),
    search_mode: str = typer.Option(
        "fts",
        "--search-mode",
        help="Search mode: fts, semantic, hybrid.",
    ),
    limit: int = typer.Option(50, "--limit", "-l", min=1, max=10000),
    offset: int = typer.Option(0, "--offset", "-o", min=0),
    metadata_keys: list[str] = typer.Option(
        [],
        "--metadata-key",
        help="Filter to metadata key (repeatable).",
    ),
    actor_ids: list[int] = typer.Option(
        [],
        "--actor-id",
        help="Filter to metadata from actor id (repeatable).",
    ),
    include_removed: bool = typer.Option(
        False, "--include-removed", help="Include removed metadata rows."
    ),
    aggregation: str = typer.Option(
        "latest",
        "--aggregation",
        help="Metadata aggregation: latest, array, objects.",
    ),
    top_k: int = typer.Option(100, "--top-k", min=1, help="Vector candidate pool size"),
    search_index: int | None = typer.Option(
        None,
        "--index",
        help="Vector index actor id (required if multiple vector index actors exist).",
    ),
    min_score: float | None = typer.Option(
        None, "--min-score", min=0.0, max=1.0, help="Filter out weaker semantic matches"
    ),
    dimension: int = typer.Option(64, "--dimension", min=1, max=4096),
    embedding_model: str = typer.Option("fast", "--embedding-model"),
    embedding_backend: str = typer.Option("preset", "--embedding-backend"),
) -> None:
    """List metadata rows, with optional semantic/hybrid ranking."""

    mode = search_mode.strip().lower()
    if mode not in {"fts", "semantic", "hybrid"}:
        raise typer.BadParameter("--search-mode must be one of: fts, semantic, hybrid")
    if mode in {"semantic", "hybrid"} and not (query or "").strip():
        raise typer.BadParameter("--query is required for semantic/hybrid mode")
    agg = aggregation.strip().lower()
    if agg not in {"latest", "array", "objects"}:
        raise typer.BadParameter("--aggregation must be one of: latest, array, objects")

    keys = metadata_keys or []
    if mode in {"semantic", "hybrid"} and not keys:
        keys = [str(DOC_CHUNK_TEXT)]

    async def _run() -> dict[str, Any]:
        from katalog.api.metadata import list_metadata as list_metadata_api

        asset_query = AssetQuery(
            view_id="default",
            search=(query or None),
            search_mode=mode,
            search_granularity="metadata",
            search_index=search_index,
            search_top_k=top_k,
            search_metadata_keys=keys or None,
            search_min_score=min_score,
            search_dimension=dimension,
            search_embedding_model=embedding_model,
            search_embedding_backend=embedding_backend,
            metadata_actor_ids=actor_ids or None,
            metadata_include_removed=include_removed,
            metadata_aggregation=agg,
            offset=offset,
            limit=limit,
        )
        return await list_metadata_api(asset_query)

    result = run_cli(_run, init_mode="fast")
    if wants_json(ctx):
        typer.echo(json.dumps(result, default=str))
        return

    items = result.get("items", [])
    stats = result.get("stats", {})
    typer.echo(f"Rows: {stats.get('returned', len(items))} / {stats.get('total', '-')}")
    typer.echo(f"Duration: {stats.get('duration_ms', '-')}ms")
    if not items:
        typer.echo("No metadata rows found")
        return

    if mode in {"semantic", "hybrid"}:
        rows = [
            {
                "score": f"{float(item.get('score', 0.0)):.3f}",
                "asset_id": str(item.get("asset_id", "-")),
                "key": str(item.get("metadata_key") or item.get("metadata_key_id") or "-"),
                "value": _text_preview(str(item.get("text") or ""), max_len=90),
            }
            for item in items
        ]
        render_table(rows, ["Score", "Asset", "Key", "Value"], ["score", "asset_id", "key", "value"])
        return

    rows = [
        {
            "asset_id": str(item.get("asset_id", "-")),
            "key": str(item.get("metadata_key") or item.get("metadata_key_id") or "-"),
            "value": _text_preview(str(item.get("text") or item.get("value") or ""), max_len=90),
            "actor_id": str(item.get("actor_id") or "-"),
        }
        for item in items
    ]
    render_table(rows, ["Asset", "Key", "Value", "Actor"], ["asset_id", "key", "value", "actor_id"])
