import asyncio
from typing import Any, Awaitable, Callable, Mapping, Sequence, TypeVar

import typer

from katalog.lifespan import InitMode, app_lifespan

T = TypeVar("T")


def run_cli(task: Callable[[], Awaitable[T]], *, init_mode: InitMode = "full") -> T:
    async def _run() -> T:
        async with app_lifespan(init_mode=init_mode):
            return await task()

    return asyncio.run(_run())


def wants_json(ctx: typer.Context) -> bool:
    return bool(ctx.obj and ctx.obj.get("json"))


def render_table(rows: Sequence[dict], headers: Sequence[str], keys: Sequence[str]) -> None:
    widths = [
        max(len(headers[i]), max(len(row[keys[i]]) for row in rows))
        for i in range(len(headers))
    ]
    header_line = "  ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    typer.echo(header_line)
    typer.echo("  ".join("-" * width for width in widths))
    for row in rows:
        typer.echo(
            "  ".join(row[keys[i]].ljust(widths[i]) for i in range(len(headers)))
        )


def format_bytes(value: Any) -> str:
    if value is None:
        return "-"
    try:
        size = float(value)
    except (TypeError, ValueError):
        return str(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit = 0
    while size >= 1024 and unit < len(units) - 1:
        size /= 1024
        unit += 1
    if unit == 0:
        return f"{int(size)} {units[unit]}"
    return f"{size:.2f} {units[unit]}"


def mapping_to_rows(
    mapping: Mapping[str, Any],
    *,
    prefix: str = "",
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in sorted(mapping.keys()):
        path = f"{prefix}.{key}" if prefix else key
        value = mapping[key]
        if isinstance(value, Mapping):
            rows.extend(mapping_to_rows(value, prefix=path))
            continue
        if isinstance(value, list):
            if all(not isinstance(item, (Mapping, list)) for item in value):
                rendered = ", ".join(str(item) for item in value)
            else:
                rendered = f"[{len(value)} items]"
            rows.append({"key": path, "value": rendered})
            continue
        rows.append({"key": path, "value": str(value)})
    return rows


def render_mapping(mapping: Mapping[str, Any], *, title: str | None = None) -> None:
    rows = mapping_to_rows(mapping)
    if title:
        typer.echo(title)
    if not rows:
        typer.echo("(empty)")
        return
    render_table(rows, ["Key", "Value"], ["key", "value"])


def changeset_summary(changeset: Any) -> dict[str, Any]:
    status = changeset.status.value if hasattr(changeset.status, "value") else str(changeset.status)
    return {
        "id": changeset.id,
        "status": status,
        "started_at": changeset.started_at_iso() if hasattr(changeset, "started_at_iso") else None,
        "elapsed_seconds": (
            changeset.running_time_ms / 1000.0
            if getattr(changeset, "running_time_ms", None) is not None
            else None
        ),
        "scan_metrics": ((changeset.data or {}).get("scan_metrics") if getattr(changeset, "data", None) else None),
        "message": getattr(changeset, "message", None),
    }


def print_changeset_summary(
    summary: Mapping[str, Any],
    *,
    label: str = "Changeset",
) -> None:
    typer.echo(f"{label}: {summary['id']}")
    if summary.get("started_at"):
        typer.echo(f"Started: {summary['started_at']}")
    typer.echo(f"Status: {summary['status']}")
    if summary.get("elapsed_seconds") is not None:
        typer.echo(f"Elapsed: {summary['elapsed_seconds']:.2f}s")
    scan_metrics = summary.get("scan_metrics")
    if scan_metrics:
        scan_seconds = scan_metrics.get("scan_seconds")
        if scan_seconds is not None:
            typer.echo(f"Scan time: {scan_seconds:.2f}s")
        for key, title in [
            ("assets_seen", "Assets seen"),
            ("assets_saved", "Assets saved"),
            ("assets_added", "Assets added"),
            ("assets_changed", "Assets changed"),
            ("assets_ignored", "Assets ignored"),
            ("assets_lost", "Assets lost"),
        ]:
            value = scan_metrics.get(key)
            if value is not None:
                typer.echo(f"{title}: {value}")
