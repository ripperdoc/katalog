import asyncio
import os
import pathlib
import shutil
import sys

import typer
from loguru import logger

app = typer.Typer(help="Katalog CLI")
actors_app = typer.Typer(help="Manage actors")
assets_app = typer.Typer(help="Manage assets")
collections_app = typer.Typer(help="Manage collections")
changesets_app = typer.Typer(help="Manage changesets")
processors_app = typer.Typer(help="Manage processors")
workflows_app = typer.Typer(help="Manage workflows")
metadata_app = typer.Typer(help="Manage metadata")
app.add_typer(actors_app, name="actors")
app.add_typer(assets_app, name="assets")
app.add_typer(collections_app, name="collections")
app.add_typer(changesets_app, name="changesets")
app.add_typer(processors_app, name="processors")
app.add_typer(workflows_app, name="workflows")
app.add_typer(metadata_app, name="metadata")


def _reset_workspace(ws: pathlib.Path) -> None:
    db_path = ws / "katalog.db"
    actors_dir = ws / "actors"
    if db_path.exists():
        db_path.unlink()
    if actors_dir.exists():
        shutil.rmtree(actors_dir)


def _set_workspace_env(ws: pathlib.Path) -> None:
    os.environ["KATALOG_WORKSPACE"] = str(ws)
    db_path = ws / "katalog.db"
    os.environ["KATALOG_DATABASE_URL"] = f"sqlite:///{db_path}"


def _ensure_src_on_path() -> None:
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[3]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[3]


def _resolve_workspace(workspace: str | None) -> pathlib.Path:
    workspace_input = workspace or os.environ.get("KATALOG_WORKSPACE")
    if not workspace_input:
        raise typer.BadParameter("provide a workspace path or set KATALOG_WORKSPACE")

    ws = pathlib.Path(workspace_input).expanduser().resolve()
    if not ws.exists() or not ws.is_dir():
        raise typer.BadParameter(f"workspace '{ws}' does not exist or is not a directory")
    return ws


async def _seed_test_workspace(ws: pathlib.Path, total_assets: int) -> None:
    from katalog.db.metadata import sync_config_db
    from katalog.models import ActorType, OpStatus
    from katalog.db.actors import get_actor_repo
    from katalog.db.changesets import get_changeset_repo
    from katalog.sources.runtime import run_sources

    await sync_config_db()

    fake_plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    db = get_actor_repo()
    actor = await db.get_or_none(plugin_id=fake_plugin_id)
    if actor is None:
        actor = await db.create(
            name="Fake Assets",
            plugin_id=fake_plugin_id,
            type=ActorType.SOURCE,
            config={"total_assets": total_assets, "seed": 1},
        )
    else:
        actor.config = {"total_assets": total_assets, "seed": 1}
        await db.save(actor)

    changeset_db = get_changeset_repo()
    changeset = await changeset_db.begin(
        actors=[actor],
        message="Test workspace seed",
        status=OpStatus.IN_PROGRESS,
    )
    status = await run_sources(sources=[actor], changeset=changeset)
    await changeset.finalize(status=status)


def _run_server(
    ws: pathlib.Path,
    *,
    port: int | None,
    test_workspace: bool,
    seed_assets: int,
    workflow_file: str | None,
    reload: bool,
    reload_dir: list[str],
) -> None:
    repo_root = _repo_root()

    _set_workspace_env(ws)
    # Change working directory so server reads workspace-local files
    os.chdir(str(ws))
    _ensure_src_on_path()

    if test_workspace:
        db_path = ws / "katalog.db"
        if db_path.exists():
            logger.info(
                "Test workspace already initialized at {db_path}; skipping reset/seed. Delete the DB to re-seed.",
                db_path=db_path,
            )
            if workflow_file:
                logger.warning(
                    "Not syncing workflow; existing DB found at {db_path}.",
                    db_path=db_path,
                )
        else:
            _reset_workspace(ws)
            if workflow_file:
                from katalog.workflows import sync_workflow_file

                asyncio.run(sync_workflow_file(pathlib.Path(workflow_file)))
            if seed_assets > 0:
                asyncio.run(_seed_test_workspace(ws, total_assets=seed_assets))
    elif seed_assets > 0:
        raise SystemExit("--seed-assets requires --test-workspace")
    elif workflow_file:
        db_path = ws / "katalog.db"
        if db_path.exists():
            logger.warning(
                "Not syncing workflow; existing DB found at {db_path}.",
                db_path=db_path,
            )
        else:
            from katalog.workflows import sync_workflow_file

            asyncio.run(sync_workflow_file(pathlib.Path(workflow_file)))

    try:
        import uvicorn
        from katalog.config import PORT

        reload_dirs = None
        if reload_dir:
            resolved = []
            for entry in reload_dir:
                path = pathlib.Path(entry)
                if not path.is_absolute():
                    path = repo_root / path
                resolved.append(str(path))
            reload_dirs = resolved

        uvicorn.run(
            "katalog.server:app",
            host="127.0.0.1",
            port=port or PORT,
            reload=bool(reload),
            reload_dirs=reload_dirs,
            access_log=False,
        )
        # Ensure the CLI process exits cleanly after uvicorn shuts down (esp. under debugpy).
        raise SystemExit(0)
    except KeyboardInterrupt:  # pragma: no cover - user initiated shutdown
        logger.info("Received interrupt signal, shutting down")
        sys.exit(0)
    except Exception:  # pragma: no cover - runtime errors
        logger.exception("Failed to start server")
        sys.exit(1)


@app.callback(invoke_without_command=True)
def cli(
    ctx: typer.Context,
    workspace_opt: str | None = typer.Option(
        None,
        "--workspace",
        "-w",
        help="Path to workspace folder to use",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output JSON instead of formatted text",
    ),
) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    if "--help" in sys.argv or "-h" in sys.argv:
        return

    ws = _resolve_workspace(workspace_opt)
    _set_workspace_env(ws)
    ctx.obj = {"workspace": ws, "json": json_output}


@app.command("server")
def server(
    ctx: typer.Context,
    port: int | None = typer.Option(
        None,
        "--port",
        help="Port to bind the server to (default: config PORT)",
    ),
    test_workspace: bool = typer.Option(
        False,
        "--test-workspace",
        help="Reset the workspace database and actor cache before starting the server",
    ),
    seed_assets: int = typer.Option(
        0,
        "--seed-assets",
        help="Seed the test workspace with this many fake assets (requires --test-workspace)",
    ),
    workflow_file: str | None = typer.Option(
        None,
        "--workflow",
        help="Sync actors from this workflow TOML before startup if no database exists",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        help="Enable auto-reload for the server (uvicorn reload)",
    ),
    reload_dir: list[str] = typer.Option(
        [],
        "--reload-dir",
        help="Directory to watch for reloads (repeatable, relative to repo root unless absolute)",
    ),
) -> None:
    ws = ctx.obj["workspace"]
    _ensure_src_on_path()
    _run_server(
        ws,
        port=port,
        test_workspace=test_workspace,
        seed_assets=seed_assets,
        workflow_file=workflow_file,
        reload=reload,
        reload_dir=reload_dir,
    )


from . import actors as _actors  # noqa: E402,F401
from . import assets as _assets  # noqa: E402,F401
from . import collections as _collections  # noqa: E402,F401
from . import changesets as _changesets  # noqa: E402,F401
from . import processors as _processors  # noqa: E402,F401
from . import workflows as _workflows  # noqa: E402,F401
from . import metadata as _metadata  # noqa: E402,F401


def main() -> None:
    app()
