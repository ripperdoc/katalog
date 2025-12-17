from argparse import ArgumentParser
from datetime import UTC, datetime
from pathlib import Path
from random import Random
import sqlite3

from tortoise import run_async
from katalog.models import MetadataType, ProviderType
from katalog.models import (
    Asset,
    AssetRelationship,
    Metadata,
    MetadataRegistry,
    Provider,
    Snapshot,
    setup,
)


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
    snapshots_per_provider: int = 3,
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

    rng = Random(seed)

    provider_types = list(ProviderType)
    if not provider_types:
        raise RuntimeError("ProviderType enum has no members")

    metadata_keys: list[str] = [
        "name",
        "mime_type",
        "size_bytes",
        "created_at",
        "modified_at",
        "md5",
        "sha256",
        "width",
        "height",
        "duration_seconds",
        "camera_make",
        "camera_model",
        "gps_lat",
        "gps_lon",
        "summary",
        "tags",
        "language",
        "author",
        "album",
        "duplicate_group",
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

    relationship_types = [
        "duplicate_of",
        "derived_from",
        "preview_of",
        "sidecar_of",
        "same_content_as",
    ]

    all_assets: list[Asset] = []

    for provider_idx in range(providers):
        provider = await Provider.create(
            title=f"provider-{provider_idx}",
            plugin_id=f"plugin-{provider_idx}",
            type=provider_types[provider_idx % len(provider_types)],
            config={"seed": seed, "provider_idx": provider_idx},
        )

        # Create the metadata key registry rows once per provider namespace.
        # In the real app, plugins should provide globally unique, namespaced keys.
        key_rows: list[MetadataRegistry] = []
        key_to_type: dict[str, MetadataType] = {}
        for short_key in metadata_keys:
            full_key = f"{provider.plugin_id}:{short_key}"
            if short_key in {"size_bytes", "width", "height"}:
                vt = MetadataType.INT
            elif short_key in {"created_at", "modified_at"}:
                vt = MetadataType.DATETIME
            elif short_key == "tags":
                vt = MetadataType.JSON
            else:
                vt = MetadataType.STRING
            key_to_type[full_key] = vt
            key_rows.append(
                MetadataRegistry(
                    key=full_key,
                    value_type=vt,
                    title=short_key,
                    description="",
                    width=None,
                )
            )
        await MetadataRegistry.bulk_create(key_rows)
        registry_rows = await MetadataRegistry.filter(key__in=list(key_to_type.keys()))
        registry_by_key = {r.key: r for r in registry_rows}

        snapshots: list[Snapshot] = []
        for snap_idx in range(snapshots_per_provider):
            snapshots.append(
                await Snapshot.create(
                    provider=provider,
                    status="completed",
                    started_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                    metadata={"snap_idx": snap_idx},
                )
            )

        created_snapshot = snapshots[0]
        last_snapshot = snapshots[-1]

        # Assets
        assets: list[Asset] = []
        for asset_idx in range(assets_per_provider):
            canonical_id = f"{provider.id}:{asset_idx}"
            canonical_uri = f"katalog://{provider.title}/asset/{asset_idx}"
            assets.append(
                Asset(
                    provider=provider,
                    canonical_id=canonical_id,
                    canonical_uri=canonical_uri,
                    created_snapshot=created_snapshot,
                    last_snapshot=last_snapshot,
                    deleted_snapshot=None,
                )
            )
        await Asset.bulk_create(assets)

        # Re-load IDs after bulk_create so we can FK from metadata/relationships.
        assets = await Asset.filter(provider=provider).order_by("id")
        all_assets.extend(assets)

        # Metadata (inserted for the last snapshot only by default)
        metadata_entries: list[Metadata] = []
        for asset in assets:
            # Ensure uniqueness for (asset, provider, snapshot, metadata_key)
            keys_for_asset = rng.sample(
                metadata_keys, k=min(metadata_per_asset, len(metadata_keys))
            )
            for key in keys_for_asset:
                registry_key = f"{provider.plugin_id}:{key}"
                registry = registry_by_key[registry_key]
                if key in {"size_bytes", "width", "height"}:
                    value_type = MetadataType.INT
                    value_int = rng.randrange(0, 5_000_000_000)
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            snapshot=last_snapshot,
                            metadata_key=registry,
                            value_type=value_type,
                            value_int=value_int,
                            removed=False,
                        )
                    )
                elif key in {"created_at", "modified_at"}:
                    value_type = MetadataType.DATETIME
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            snapshot=last_snapshot,
                            metadata_key=registry,
                            value_type=value_type,
                            value_datetime=datetime.now(UTC),
                            removed=False,
                        )
                    )
                elif key == "mime_type":
                    value_type = MetadataType.STRING
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            snapshot=last_snapshot,
                            metadata_key=registry,
                            value_type=value_type,
                            value_text=_pick_weighted(rng, mime_types),
                            removed=False,
                        )
                    )
                elif key in {"tags"}:
                    value_type = MetadataType.JSON
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
                    value_json = rng.sample(tag_pool, k=rng.randrange(0, 5))
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            snapshot=last_snapshot,
                            metadata_key=registry,
                            value_type=value_type,
                            value_json=value_json,
                            removed=False,
                        )
                    )
                else:
                    value_type = MetadataType.STRING
                    # Intentionally repetitive-ish strings.
                    metadata_entries.append(
                        Metadata(
                            asset=asset,
                            provider=provider,
                            snapshot=last_snapshot,
                            metadata_key=registry,
                            value_type=value_type,
                            value_text=f"{key}-{rng.randrange(0, 200)}",
                            removed=False,
                        )
                    )
        if metadata_entries:
            await Metadata.bulk_create(metadata_entries)

        # Relationships
        rels: list[AssetRelationship] = []
        if len(assets) >= 2 and relationships_per_asset > 0:
            for asset in assets:
                for _ in range(relationships_per_asset):
                    other = assets[rng.randrange(0, len(assets))]
                    if other.id == asset.id:
                        continue
                    rels.append(
                        AssetRelationship(
                            provider=provider,
                            from_asset=asset,
                            to_asset=other,
                            relationship_type=relationship_types[
                                rng.randrange(0, len(relationship_types))
                            ],
                            snapshot=last_snapshot,
                            removed=False,
                            description=None,
                        )
                    )
        if rels:
            await AssetRelationship.bulk_create(rels)

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
            "asset.avg_len(canonical_id)": _try_scalar(
                "select avg(length(canonical_id)) from asset"
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
    parser.add_argument("--db", type=Path, default=Path("katalog_database.sqlite3"))
    parser.add_argument("--populate", action="store_true")
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--providers", type=int, default=1)
    parser.add_argument("--snapshots-per-provider", type=int, default=3)
    parser.add_argument("--assets-per-provider", type=int, default=50_000)
    parser.add_argument("--metadata-per-asset", type=int, default=25)
    parser.add_argument("--relationships-per-asset", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1)
    return parser.parse_args()


async def main_async(args: object) -> None:
    # argparse.Namespace, but keep it untyped to avoid importing typing_extensions.
    if getattr(args, "populate"):
        await populate_test_data(
            getattr(args, "db"),
            providers=getattr(args, "providers"),
            snapshots_per_provider=getattr(args, "snapshots_per_provider"),
            assets_per_provider=getattr(args, "assets_per_provider"),
            metadata_per_asset=getattr(args, "metadata_per_asset"),
            relationships_per_asset=getattr(args, "relationships_per_asset"),
            seed=getattr(args, "seed"),
        )
    else:
        await setup(getattr(args, "db"))


def cli() -> None:
    args = _parse_args()

    if getattr(args, "analyze"):
        analyze_sqlite(getattr(args, "db"))
        return

    run_async(main_async(args))


if __name__ == "__main__":
    cli()
