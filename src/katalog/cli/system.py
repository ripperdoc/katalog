import json
from typing import Any

import typer

from . import app
from .utils import (
    format_bytes,
    render_mapping,
    render_table,
    run_cli,
    wants_json,
)


@app.command("stats")
def workspace_stats(ctx: typer.Context) -> None:
    """Show workspace and database size statistics."""

    async def _run() -> dict[str, Any]:
        from katalog.api.system import workspace_size_stats as workspace_size_stats_api

        return await workspace_size_stats_api()

    stats = run_cli(_run, runtime_mode="fast_read")
    if wants_json(ctx):
        typer.echo(json.dumps(stats, default=str))
        return

    summary = stats.get("summary") if isinstance(stats, dict) else None
    if isinstance(summary, dict):
        render_mapping(summary, title="Summary")
        typer.echo("")

    workspace = stats.get("workspace") if isinstance(stats, dict) else None
    if isinstance(workspace, dict):
        workspace_meta = {k: v for k, v in workspace.items() if k != "entries"}
        render_mapping(workspace_meta, title="Workspace")
        typer.echo("")

    database = stats.get("database") if isinstance(stats, dict) else None
    if isinstance(database, dict):
        database_meta = {k: v for k, v in database.items() if k not in {"tables", "indexes"}}
        render_mapping(database_meta, title="Database")
        typer.echo("")

        tables = database.get("tables")
        if isinstance(tables, list) and tables:
            rows = []
            for table in tables:
                if not isinstance(table, dict):
                    continue
                row_count = table.get("row_count")
                row_count_text = "-" if row_count is None else str(row_count)
                if row_count is None and table.get("row_count_error"):
                    row_count_text = "error"
                rows.append(
                    {
                        "name": str(table.get("name") or ""),
                        "rows": row_count_text,
                        "size": format_bytes(table.get("size_bytes")),
                    }
                )
            if rows:
                typer.echo("Tables")
                render_table(rows, ["Table", "Rows", "Size"], ["name", "rows", "size"])
                typer.echo("")

        indexes = database.get("indexes")
        if isinstance(indexes, list) and indexes:
            rows = []
            for index in indexes:
                if not isinstance(index, dict):
                    continue
                rows.append(
                    {
                        "name": str(index.get("name") or ""),
                        "size": format_bytes(index.get("size_bytes")),
                    }
                )
            if rows:
                typer.echo("Indexes")
                render_table(rows, ["Index", "Size"], ["name", "size"])
