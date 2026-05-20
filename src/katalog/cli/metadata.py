import json
import re
from typing import Any

import asyncclick as click

from katalog.constants.metadata import DOC_CHUNK_TEXT
from katalog.models.query import AssetQuery

from . import metadata_app
from .utils import render_table, wants_json, with_lifespan


def _text_preview(value: str, *, max_len: int = 90) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


@metadata_app.command("list")
@click.option(
    "--query",
    "query",
    "-q",
    default=None,
    help="Search text (required for semantic/hybrid mode).",
)
@click.option(
    "--search-mode",
    type=str,
    default="fts",
    show_default=True,
    help="Search mode: fts, semantic, hybrid.",
)
@click.option("--limit", "-l", type=click.IntRange(min=1, max=10000), default=50, show_default=True)
@click.option("--offset", "-o", type=click.IntRange(min=0), default=0, show_default=True)
@click.option(
    "--metadata-key",
    "metadata_keys",
    multiple=True,
    help="Filter to metadata key (repeatable).",
)
@click.option(
    "--actor-id",
    "actor_ids",
    type=int,
    multiple=True,
    help="Filter to metadata from actor id (repeatable).",
)
@click.option(
    "--include-removed",
    is_flag=True,
    default=False,
    help="Include removed metadata rows.",
)
@click.option(
    "--aggregation",
    type=str,
    default="latest",
    show_default=True,
    help="Metadata aggregation: latest, current, object.",
)
@click.option(
    "--top-k",
    type=click.IntRange(min=1),
    default=100,
    show_default=True,
    help="Vector candidate pool size",
)
@click.option(
    "--index",
    "search_index",
    type=int,
    default=None,
    help="Vector index actor id (required if multiple vector index actors exist).",
)
@click.option(
    "--min-score",
    type=click.FloatRange(min=0.0, max=1.0),
    default=None,
    help="Filter out weaker semantic matches",
)
@click.option(
    "--dimension",
    type=click.IntRange(min=1, max=4096),
    default=64,
    show_default=True,
)
@click.option("--embedding-model", type=str, default="fast", show_default=True)
@click.option("--embedding-backend", type=str, default="preset", show_default=True)
@click.option(
    "--view-id",
    type=str,
    default="default",
    show_default=True,
    help="Asset view id context for query defaults (run `katalog views list` to discover ids).",
)
@with_lifespan(runtime_mode="fast_read")
async def list_metadata(
    ctx: click.Context,
    query: str | None,
    search_mode: str,
    limit: int,
    offset: int,
    metadata_keys: tuple[str, ...],
    actor_ids: tuple[int, ...],
    include_removed: bool,
    aggregation: str,
    top_k: int,
    search_index: int | None,
    min_score: float | None,
    dimension: int,
    embedding_model: str,
    embedding_backend: str,
    view_id: str,
) -> None:
    """List metadata rows, with optional semantic/hybrid ranking."""

    mode = search_mode.strip().lower()
    if mode not in {"fts", "semantic", "hybrid"}:
        raise click.BadParameter("--search-mode must be one of: fts, semantic, hybrid")
    if mode in {"semantic", "hybrid"} and not (query or "").strip():
        raise click.BadParameter("--query is required for semantic/hybrid mode")
    agg = aggregation.strip().lower()
    if agg not in {"latest", "current", "object"}:
        raise click.BadParameter("--aggregation must be one of: latest, current, object")

    keys = list(metadata_keys or ())
    if mode in {"semantic", "hybrid"} and not keys:
        keys = [str(DOC_CHUNK_TEXT)]

    from katalog.api.metadata import list_metadata as list_metadata_api

    asset_query = AssetQuery(
        view_id=view_id,
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
        metadata_actor_ids=list(actor_ids) or None,
        metadata_include_removed=include_removed,
        metadata_aggregation=agg,
        offset=offset,
        limit=limit,
    )
    result = await list_metadata_api(asset_query)

    if wants_json(ctx):
        click.echo(json.dumps(result, default=str))
        return

    items = result.get("items", [])
    stats = result.get("stats", {})
    click.echo(f"Rows: {stats.get('returned', len(items))} / {stats.get('total', '-')}")
    click.echo(f"Duration: {stats.get('duration_ms', '-')}ms")
    if not items:
        click.echo("No metadata rows found")
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
