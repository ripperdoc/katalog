import argparse
import os
import sys
import pathlib

from loguru import logger

from katalog.config import PORT


def main():
    parser = argparse.ArgumentParser(
        prog="katalog", description="Start katalog server for a workspace"
    )
    parser.add_argument(
        "workspace",
        nargs="?",
        help="Path to workspace folder to use (also settable via KATALOG_WORKSPACE)",
    )
    args = parser.parse_args()

    workspace_input = args.workspace or os.environ.get("KATALOG_WORKSPACE")
    if not workspace_input:
        parser.error("provide a workspace path or set KATALOG_WORKSPACE")

    ws = pathlib.Path(workspace_input).expanduser().resolve()
    if not ws.exists() or not ws.is_dir():
        logger.error(f"Workspace '{ws}' does not exist or is not a directory")
        sys.exit(2)

    os.environ["KATALOG_WORKSPACE"] = str(ws)
    if "KATALOG_DATABASE_URL" not in os.environ:
        db_path = ws / "katalog.db"
        os.environ["KATALOG_DATABASE_URL"] = f"sqlite:///{db_path}"

    # Change working directory so server reads workspace-local files
    os.chdir(str(ws))

    # If the project layout includes a top-level `src` directory, ensure it is on sys.path
    # This helps when running the CLI from source without installing the package.
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[2]
    src_dir = repo_root / "src"
    if src_dir.exists():
        sys.path.insert(0, str(src_dir))

    try:
        import uvicorn

        # Run uvicorn by import string so reload stays disabled by default
        uvicorn.run(
            "katalog.server:app",
            host="127.0.0.1",
            port=PORT,
            reload=False,
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
