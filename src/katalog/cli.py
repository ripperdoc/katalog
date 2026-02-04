import argparse
import asyncio
import os
import pathlib
import shutil
import sys

from loguru import logger


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="katalog", description="Start katalog server for a workspace"
    )
    parser.add_argument(
        "workspace",
        nargs="?",
        help="Path to workspace folder to use (also settable via KATALOG_WORKSPACE)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to bind the server to (default: config PORT)",
    )
    parser.add_argument(
        "--test-workspace",
        action="store_true",
        help="Reset the workspace database and actor cache before starting the server",
    )
    parser.add_argument(
        "--seed-assets",
        type=int,
        default=0,
        help="Seed the test workspace with this many fake assets (requires --test-workspace)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for the server (uvicorn reload)",
    )
    parser.add_argument(
        "--reload-dir",
        action="append",
        default=[],
        help="Directory to watch for reloads (repeatable, relative to repo root unless absolute)",
    )
    return parser.parse_args()


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
    repo_root = here.parents[2]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))


def _repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[2]


def _validate_workspace(args: argparse.Namespace) -> pathlib.Path:
    workspace_input = args.workspace or os.environ.get("KATALOG_WORKSPACE")
    if not workspace_input:
        raise SystemExit("provide a workspace path or set KATALOG_WORKSPACE")

    ws = pathlib.Path(workspace_input).expanduser().resolve()
    if not ws.exists() or not ws.is_dir():
        logger.error(f"Workspace '{ws}' does not exist or is not a directory")
        sys.exit(2)
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


def main() -> None:
    args = _parse_args()
    ws = _validate_workspace(args)
    repo_root = _repo_root()

    _set_workspace_env(ws)
    # Change working directory so server reads workspace-local files
    os.chdir(str(ws))
    _ensure_src_on_path()

    if args.test_workspace:
        db_path = ws / "katalog.db"
        if db_path.exists():
            logger.info(
                "Test workspace already initialized at {db_path}; skipping reset/seed. Delete the DB to re-seed.",
                db_path=db_path,
            )
        else:
            _reset_workspace(ws)
            if args.seed_assets > 0:
                asyncio.run(_seed_test_workspace(ws, total_assets=args.seed_assets))
    elif args.seed_assets > 0:
        raise SystemExit("--seed-assets requires --test-workspace")

    try:
        import uvicorn
        from katalog.config import PORT

        reload_dirs = None
        if args.reload_dir:
            resolved = []
            for entry in args.reload_dir:
                path = pathlib.Path(entry)
                if not path.is_absolute():
                    path = repo_root / path
                resolved.append(str(path))
            reload_dirs = resolved

        uvicorn.run(
            "katalog.server:app",
            host="127.0.0.1",
            port=args.port or PORT,
            reload=bool(args.reload),
            reload_dirs=reload_dirs,
            access_log=False,
        )
    except KeyboardInterrupt:  # pragma: no cover - user initiated shutdown
        logger.info("Received interrupt signal, shutting down")
        sys.exit(0)
    except Exception:  # pragma: no cover - runtime errors
        logger.exception("Failed to start server")
        sys.exit(1)


if __name__ == "__main__":
    main()
