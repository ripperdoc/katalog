import asyncio
import os
import pathlib
import shutil
import sys

import asyncclick as click
from loguru import logger

from katalog.help_texts import (
    ACTORS_GROUP_HELP,
    ASSETS_GROUP_HELP,
    CHANGESETS_GROUP_HELP,
    CLI_APP_HELP,
    COLLECTIONS_GROUP_HELP,
    JSON_OPTION_HELP,
    METADATA_GROUP_HELP,
    PROCESSORS_GROUP_HELP,
    READ_ONLY_OPTION_HELP,
    SERVER_COMMAND_HELP,
    VIEWS_GROUP_HELP,
    WORKFLOWS_GROUP_HELP,
    WORKSPACE_OPTION_HELP,
)


@click.group(help=CLI_APP_HELP, invoke_without_command=True)
@click.option(
    "--workspace",
    "workspace_opt",
    "-w",
    default=None,
    help=WORKSPACE_OPTION_HELP,
)
@click.option(
    "--json",
    "json_output",
    is_flag=True,
    default=False,
    help=JSON_OPTION_HELP,
)
@click.option(
    "--read-only",
    "read_only_opt",
    is_flag=True,
    default=False,
    help=READ_ONLY_OPTION_HELP,
)
@click.pass_context
async def app(
    ctx: click.Context,
    workspace_opt: str | None,
    json_output: bool,
    read_only_opt: bool,
) -> None:
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        raise click.exceptions.Exit()

    # Allow help output without requiring workspace to resolve.
    if "--help" in sys.argv or "-h" in sys.argv:
        return

    ws = _resolve_workspace(workspace_opt)
    _set_workspace_env(ws)
    if read_only_opt:
        os.environ["KATALOG_READ_ONLY"] = "1"
    else:
        os.environ.pop("KATALOG_READ_ONLY", None)
    ctx.obj = {"workspace": ws, "json": json_output, "read_only": read_only_opt}


@app.group("actors", help=ACTORS_GROUP_HELP)
async def actors_app() -> None:
    """Actors command group."""


@app.group("assets", help=ASSETS_GROUP_HELP)
async def assets_app() -> None:
    """Assets command group."""


@app.group("collections", help=COLLECTIONS_GROUP_HELP)
async def collections_app() -> None:
    """Collections command group."""


@app.group("changesets", help=CHANGESETS_GROUP_HELP)
async def changesets_app() -> None:
    """Changesets command group."""


@app.group("processors", help=PROCESSORS_GROUP_HELP)
async def processors_app() -> None:
    """Processors command group."""


@app.group("workflows", help=WORKFLOWS_GROUP_HELP)
async def workflows_app() -> None:
    """Workflows command group."""


@app.group("metadata", help=METADATA_GROUP_HELP)
async def metadata_app() -> None:
    """Metadata command group."""


@app.group("views", help=VIEWS_GROUP_HELP)
async def views_app() -> None:
    """Views command group."""


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
        raise click.BadParameter("provide a workspace path or set KATALOG_WORKSPACE")

    ws = pathlib.Path(workspace_input).expanduser().resolve()
    if not ws.exists() or not ws.is_dir():
        raise click.BadParameter(f"workspace '{ws}' does not exist or is not a directory")
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
    read_only: bool,
    with_mcp: bool,
    test_workspace: bool,
    seed_assets: int,
    workflow_file: str | None,
    reload: bool,
    reload_dir: tuple[str, ...],
) -> None:
    repo_root = _repo_root()

    _set_workspace_env(ws)
    if read_only:
        os.environ["KATALOG_READ_ONLY"] = "1"
    else:
        os.environ.pop("KATALOG_READ_ONLY", None)
    if with_mcp:
        os.environ["KATALOG_ENABLE_MCP"] = "1"
    else:
        os.environ.pop("KATALOG_ENABLE_MCP", None)
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

        bind_port = port or PORT
        base_url = f"http://127.0.0.1:{bind_port}"
        logger.info("Server URL: {}", base_url)
        logger.info("UI URL: {}", f"{base_url}/")
        logger.info("API URL: {}", f"{base_url}/api")
        logger.info("Docs URL: {}", f"{base_url}/docs")

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
            port=bind_port,
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


@app.command("server", help=SERVER_COMMAND_HELP)
@click.option(
    "--port",
    default=None,
    type=int,
    help="Port to bind the server to (default: config PORT)",
)
@click.option(
    "--read-only",
    "server_read_only",
    is_flag=True,
    default=False,
    help="Run server startup/runtime context in read-only mode",
)
@click.option(
    "--with-mcp",
    is_flag=True,
    default=False,
    help="Enable MCP endpoint at /mcp in the same server process",
)
@click.option(
    "--test-workspace",
    is_flag=True,
    default=False,
    help="Reset the workspace database and actor cache before starting the server",
)
@click.option(
    "--seed-assets",
    default=0,
    type=int,
    help="Seed the test workspace with this many fake assets (requires --test-workspace)",
)
@click.option(
    "--workflow",
    "workflow_file",
    default=None,
    help="Sync actors from this workflow TOML before startup if no database exists",
)
@click.option(
    "--reload",
    is_flag=True,
    default=False,
    help="Enable auto-reload for the server (uvicorn reload)",
)
@click.option(
    "--reload-dir",
    "reload_dir",
    multiple=True,
    help="Directory to watch for reloads (repeatable, relative to repo root unless absolute)",
)
@click.pass_context
async def server(
    ctx: click.Context,
    port: int | None,
    server_read_only: bool,
    with_mcp: bool,
    test_workspace: bool,
    seed_assets: int,
    workflow_file: str | None,
    reload: bool,
    reload_dir: tuple[str, ...],
) -> None:
    ws = ctx.obj["workspace"]
    _ensure_src_on_path()
    requested_read_only = bool(ctx.obj.get("read_only")) or bool(server_read_only)
    _run_server(
        ws,
        port=port,
        read_only=requested_read_only,
        with_mcp=with_mcp,
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
from . import views as _views  # noqa: E402,F401
from . import system as _system  # noqa: E402,F401


def main() -> None:
    app()
