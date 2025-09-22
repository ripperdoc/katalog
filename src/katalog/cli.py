import argparse
import os
import sys
import pathlib


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
        print(
            f"Error: workspace '{ws}' does not exist or is not a directory",
            file=sys.stderr,
        )
        sys.exit(2)

    os.environ["KATALOG_WORKSPACE"] = str(ws)
    if "KATALOG_DATABASE_URL" not in os.environ:
        db_path = ws / "katalog.db"
        os.environ["KATALOG_DATABASE_URL"] = f"sqlite:///{db_path}"

    # Change working directory so server reads workspace-local files (e.g. katalog.toml)
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

        # Run uvicorn by import string so `reload=True` works (reload requires import string)
        # Pass `reload_dirs` to watch the repository `src/` directory for changes
        uvicorn.run(
            "katalog.server:app",
            host="127.0.0.1",
            port=8000,
            reload=True,
            reload_dirs=[str(src_dir)],
        )
    except Exception as exc:  # pragma: no cover - runtime errors
        print("Failed to start server:", exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
