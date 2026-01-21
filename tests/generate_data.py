from argparse import ArgumentParser
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from random import Random
from time import time
import sqlite3
import shutil

from tortoise import run_async
from katalog.models import MetadataType, OpStatus, ProviderType
from katalog.models import Asset, Metadata, MetadataRegistry, Provider, Changeset
from katalog.metadata import (
    ACCESS_OWNER,
    ACCESS_SHARED_WITH,
    DOC_LANG,
    FILE_EXTENSION,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    FILE_TAGS,
    FILE_TYPE,
    FILE_URI,
    HASH_MD5,
    HASH_SHA1,
    TIME_ACCESSED,
    TIME_CREATED,
    TIME_MODIFIED,
)
from katalog.queries import setup_db


def _pick_weighted(rng: Random, items: list[tuple[str, float]]) -> str:
    total = sum(weight for _, weight in items)
    needle = rng.random() * total
    acc = 0.0
    for item, weight in items:
        acc += weight
        if needle <= acc:
            return item
    return items[-1][0]


def _format_bytes(num_bytes: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TiB"


async def populate_test_data(
    db_path: Path,
    *,
    providers: int = 1,
    changesets_per_provider: int = 3,
    assets_per_provider: int = 1_000,
    metadata_per_asset: int = 20,
    relationships_per_asset: int = 2,
    seed: int = 1,
) -> Path:
    """Populate the SQLite DB with synthetic data for storage/format testing.

    Notes:
    - Keeps strings intentionally repetitive to reflect real metadata patterns.
    - Uses deterministic generation via `seed`.
    - Defaults are small enough to run quickly; scale up intentionally.
    """

    workspace_dir = db_path.expanduser().resolve().parent
    reset_workspace = workspace_dir.exists() and workspace_dir.name == "test_workspace"
    if reset_workspace:
        shutil.rmtree(workspace_dir)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    if reset_workspace:
        print(f"Reset workspace at {workspace_dir}")
    else:
        print(f"Preparing workspace at {workspace_dir}")

    await setup_db(db_path)

    rng = Random(seed)

    provider_types = list(ProviderType)
    if not provider_types:
        raise RuntimeError("ProviderType enum has no members")

    # Core metadata keys to populate (covers UI fields plus a few extras)
    metadata_keys = [
        FILE_PATH,
        FILE_NAME,
        FILE_SIZE,
        FILE_TYPE,
        FILE_EXTENSION,
        FILE_URI,
        TIME_CREATED,
        TIME_MODIFIED,
        TIME_ACCESSED,
        ACCESS_OWNER,
        ACCESS_SHARED_WITH,
        FILE_TAGS,
        HASH_MD5,
        HASH_SHA1,
        DOC_LANG,
    ]

    mime_types = [
        ("image/jpeg", 0.35),
        ("image/png", 0.15),
        ("application/pdf", 0.10),
        ("video/mp4", 0.10),
        ("text/plain", 0.08),
        ("application/zip", 0.05),
        ("audio/mpeg", 0.04),
        ("application/octet-stream", 0.03),
        ("image/heic", 0.03),
        ("text/markdown", 0.02),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            0.02,
        ),
        ("application/vnd.ms-excel", 0.01),
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 0.01),
        ("application/vnd.apple.keynote", 0.01),
    ]

    total_metadata_entries = 0

    for provider_idx in range(providers):
        provider = await Provider.create(
            name=f"provider-{provider_idx}",
            plugin_id=f"plugin-{provider_idx}",
            type=provider_types[provider_idx % len(provider_types)],
            config={"seed": seed, "provider_idx": provider_idx},
        )

        registry_rows = await MetadataRegistry.filter(
            key__in=[str(k) for k in metadata_keys]
        )
        registry_by_key = {r.key: r for r in registry_rows}

        changesets: list[Changeset] = []
        base_changeset_id = int(time()) + provider_idx * 10_000
        for snap_idx in range(changesets_per_provider):
            changesets.append(
                await Changeset.create(
                    id=base_changeset_id + snap_idx,
                    provider=provider,
                    status=OpStatus.COMPLETED,
                    started_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                    metadata={"snap_idx": snap_idx},
                )
            )

        created_changeset = changesets[0]
        last_changeset = changesets[-1]

        # Assets
        assets: list[Asset] = []
        for asset_idx in range(assets_per_provider):
            external_id = f"{provider.id}:{asset_idx}"
            canonical_uri = f"katalog://{provider.name}/asset/{asset_idx}"
            asset = Asset(
                external_id=external_id,
                canonical_uri=canonical_uri,
            )
            await asset.save_record(changeset=created_changeset, provider=provider)
            if last_changeset.id != created_changeset.id:
                await asset.save_record(changeset=last_changeset, provider=provider)
            assets.append(asset)

        # Metadata (inserted for the last changeset only by default)
        metadata_entries: list[Metadata] = []
        tag_pool = [
            "work",
            "personal",
            "invoice",
            "photo",
            "scan",
            "archive",
            "music",
            "video",
        ]

        for asset in assets:
            keys_for_asset = rng.sample(
                metadata_keys, k=min(metadata_per_asset, len(metadata_keys))
            )
            for key in keys_for_asset:
                registry = registry_by_key[str(key)]
                value_type = registry.value_type

                if value_type == MetadataType.INT:
                    if registry.key == str(FILE_SIZE):
                        value_int = rng.randrange(0, 5_000_000_000)
                    else:
                        value_int = rng.randrange(0, 10_000)
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            changeset=last_changeset,
                            metadata_key=registry,
                            value_type=value_type,
                            value_int=value_int,
                            removed=False,
                        )
                    )
                elif value_type == MetadataType.DATETIME:
                    tz_offsets = [-12, -5, 0, 3, 5, 9, 12]
                    offset_hours = rng.choice(tz_offsets)
                    tz = timezone(timedelta(hours=offset_hours))
                    start = datetime(2020, 1, 1, tzinfo=UTC)
                    end = datetime.now(UTC)
                    span_seconds = (end - start).total_seconds()
                    chosen = start + timedelta(seconds=int(rng.random() * span_seconds))
                    aware_dt = chosen.astimezone(tz)
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            changeset=last_changeset,
                            metadata_key=registry,
                            value_type=value_type,
                            value_datetime=aware_dt,
                            removed=False,
                        )
                    )
                elif value_type == MetadataType.JSON:
                    value_json = rng.sample(tag_pool, k=rng.randrange(0, 5))
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            changeset=last_changeset,
                            metadata_key=registry,
                            value_type=value_type,
                            value_json=value_json,
                            removed=False,
                        )
                    )
                else:
                    if registry.key == str(FILE_TYPE):
                        value_text = _pick_weighted(rng, mime_types)
                    elif registry.key == str(HASH_MD5):
                        value_text = f"md5-{rng.randrange(0, 1_000_000):06x}"
                    elif registry.key == str(HASH_SHA1):
                        value_text = f"sha1-{rng.randrange(0, 1_000_000):06x}"
                    elif registry.key == str(FILE_NAME):
                        value_text = f"asset-{asset.id}.dat"
                    else:
                        value_text = f"{registry.key}-{rng.randrange(0, 200)}"
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            changeset=last_changeset,
                            metadata_key=registry,
                            value_type=value_type,
                            value_text=value_text,
                            removed=False,
                        )
                    )
        if metadata_entries:
            total_metadata_entries += len(metadata_entries)
            await Metadata.bulk_create(metadata_entries)

    total_assets = providers * assets_per_provider
    total_changesets = providers * changesets_per_provider
    print(
        f"Generated {total_assets} assets and {total_metadata_entries} metadata rows "
        f"across {providers} providers and {total_changesets} changesets"
    )
    print(f"Database written to {db_path.resolve()}")

    return db_path


def analyze_sqlite(db_path: Path) -> None:
    """Print a compact size breakdown for a SQLite database.

    Uses dbstat if available (best signal for table/index bloat).
    """

    db_path = db_path.expanduser().resolve()
    wal_path = Path(f"{db_path}-wal")
    shm_path = Path(f"{db_path}-shm")

    if not db_path.exists():
        raise FileNotFoundError(str(db_path))

    db_size = db_path.stat().st_size
    wal_size = wal_path.stat().st_size if wal_path.exists() else 0
    shm_size = shm_path.stat().st_size if shm_path.exists() else 0

    print(f"DB: {db_path}")
    print(
        f"Files: db={_format_bytes(db_size)}, wal={_format_bytes(wal_size)}, shm={_format_bytes(shm_size)}"
    )

    con = sqlite3.connect(str(db_path))
    try:
        con.row_factory = sqlite3.Row
        sqlite_version = con.execute("select sqlite_version()").fetchone()[0]
        print(f"SQLite: {sqlite_version}")

        page_size = con.execute("pragma page_size").fetchone()[0]
        page_count = con.execute("pragma page_count").fetchone()[0]
        freelist = con.execute("pragma freelist_count").fetchone()[0]
        print(
            f"Pages: size={page_size} bytes, count={page_count}, freelist={freelist}, approx={_format_bytes(page_size * page_count)}"
        )

        # Row counts (coarse but helpful)
        tables = [
            r[0]
            for r in con.execute(
                "select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name"
            ).fetchall()
        ]
        if tables:
            print("Rows:")
            for table in tables:
                try:
                    cnt = con.execute(f"select count(*) from {table}").fetchone()[0]
                except sqlite3.OperationalError:
                    continue
                print(f"  {table}: {cnt}")

        # Best-effort size breakdown via dbstat.
        print("Largest objects:")
        try:
            con.execute("create virtual table temp._dbstat using dbstat")
            rows = con.execute(
                """
                select
                  s.name as name,
                  m.type as master_type,
                  sum(s.pgsize) as bytes
                from temp._dbstat s
                left join sqlite_master m on m.name = s.name
                group by s.name
                order by bytes desc
                limit 15
                """
            ).fetchall()
            for r in rows:
                obj_type = r["master_type"] or "(internal)"
                print(f"  {r['name']} [{obj_type}]: {_format_bytes(int(r['bytes']))}")
        except sqlite3.OperationalError as e:
            print(f"  (dbstat unavailable: {e})")

        # Index inventory (helps identify which long-text fields are being indexed)
        print("Indexes:")
        for table in tables:
            try:
                idxs = con.execute(f"pragma index_list('{table}')").fetchall()
            except sqlite3.OperationalError:
                continue
            for idx in idxs:
                idx_name = idx[1]
                is_unique = bool(idx[2])
                try:
                    cols = [
                        r[2]
                        for r in con.execute(
                            f"pragma index_info('{idx_name}')"
                        ).fetchall()
                    ]
                except sqlite3.OperationalError:
                    cols = []
                uniq = "unique" if is_unique else "non-unique"
                cols_str = ",".join(cols) if cols else "(unknown)"
                print(f"  {table}.{idx_name} [{uniq}]: {cols_str}")

        # A few high-signal column size stats (can take a bit on huge DBs).
        def _try_scalar(sql: str) -> str | None:
            try:
                val = con.execute(sql).fetchone()[0]
            except sqlite3.OperationalError:
                return None
            if val is None:
                return None
            if isinstance(val, float):
                return f"{val:.2f}"
            return str(val)

        stats = {
            "asset.avg_len(external_id)": _try_scalar(
                "select avg(length(external_id)) from asset"
            ),
            "asset.avg_len(canonical_uri)": _try_scalar(
                "select avg(length(canonical_uri)) from asset"
            ),
            "metadata_key_registry.avg_len(key)": _try_scalar(
                "select avg(length(key)) from metadatakeyregistry"
            ),
            "metadata.avg_len(value_text)": _try_scalar(
                "select avg(length(value_text)) from metadata where value_text is not null"
            ),
            "rel.avg_len(relationship_type)": _try_scalar(
                "select avg(length(relationship_type)) from assetrelationship"
            ),
        }
        emitted = False
        for k, v in stats.items():
            if v is None:
                continue
            if not emitted:
                print("Length stats:")
                emitted = True
            print(f"  {k}: {v}")

        # For large metadata tables, the per-type distribution is usually highly informative.
        try:
            rows = con.execute(
                "select value_type, count(*) as cnt from metadata group by value_type order by cnt desc"
            ).fetchall()
            if rows:
                print("Metadata by value_type:")
                for r in rows:
                    print(f"  {r[0]}: {r[1]}")
        except sqlite3.OperationalError:
            pass
    finally:
        con.close()


def _parse_args() -> object:
    parser = ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("test_workspace/katalog.db"))
    parser.add_argument(
        "--populate",
        action="store_true",
        help="Populate the database (default behavior).",
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only initialize the schema without inserting data.",
    )
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--providers", type=int, default=1)
    parser.add_argument("--changesets-per-provider", type=int, default=3)
    parser.add_argument("--assets-per-provider", type=int, default=5_000)
    parser.add_argument("--metadata-per-asset", type=int, default=25)
    parser.add_argument("--relationships-per-asset", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1)
    return parser.parse_args()


async def main_async(args: object) -> None:
    # argparse.Namespace, but keep it untyped to avoid importing typing_extensions.
    populate_flag = getattr(args, "populate")
    setup_only = getattr(args, "setup_only")
    if populate_flag or not setup_only:
        await populate_test_data(
            getattr(args, "db"),
            providers=getattr(args, "providers"),
            changesets_per_provider=getattr(args, "changesets_per_provider"),
            assets_per_provider=getattr(args, "assets_per_provider"),
            metadata_per_asset=getattr(args, "metadata_per_asset"),
            relationships_per_asset=getattr(args, "relationships_per_asset"),
            seed=getattr(args, "seed"),
        )
    else:
        await setup_db(getattr(args, "db"))


def cli() -> None:
    args = _parse_args()

    if getattr(args, "analyze"):
        analyze_sqlite(getattr(args, "db"))
        return

    run_async(main_async(args))


if __name__ == "__main__":
    cli()
