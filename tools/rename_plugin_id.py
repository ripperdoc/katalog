#!/usr/bin/env python3
"""Rename actor plugin ids in a katalog.db.

Usage:
    uv run python tools/rename_plugin_id.py /path/to/katalog.db \
      --from-plugin katalog.processors.search_index.SearchIndexProcessor \
      --to-plugin katalog.processors.search_index.FullTextSearchIndexProcessor

Dry-run mode is enabled by default. Pass --apply to persist changes.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def rename_plugin_id(
    *,
    db_path: Path,
    from_plugin: str,
    to_plugin: str,
    apply: bool,
) -> int:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT id, name, plugin_id, type, disabled
            FROM actors
            WHERE plugin_id = ?
            ORDER BY id ASC
            """,
            (from_plugin,),
        ).fetchall()

        if not rows:
            return 0

        print(f"Found {len(rows)} actor(s) using plugin_id={from_plugin!r}")
        for row in rows:
            print(
                f" - actor_id={row[0]} name={row[1]!r} type={row[3]} disabled={row[4]}"
            )

        if not apply:
            print("Dry run only. No changes written. Use --apply to persist.")
            return len(rows)

        conn.execute("BEGIN")
        conn.execute(
            "UPDATE actors SET plugin_id = ? WHERE plugin_id = ?",
            (to_plugin, from_plugin),
        )
        conn.execute("COMMIT")
        print(f"Updated {len(rows)} actor(s) to plugin_id={to_plugin!r}")
        return len(rows)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rename actors.plugin_id values in a katalog.db"
    )
    parser.add_argument("db_path", type=Path, help="Path to katalog.db")
    parser.add_argument("--from-plugin", required=True, help="Plugin id to rename from")
    parser.add_argument("--to-plugin", required=True, help="Plugin id to rename to")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist changes (default is dry run)",
    )
    args = parser.parse_args()

    if args.from_plugin == args.to_plugin:
        raise ValueError("--from-plugin and --to-plugin must be different")

    rename_plugin_id(
        db_path=args.db_path.resolve(),
        from_plugin=args.from_plugin,
        to_plugin=args.to_plugin,
        apply=bool(args.apply),
    )


if __name__ == "__main__":
    main()
