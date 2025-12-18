from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, UTC
from enum import Enum, IntEnum
from pathlib import Path

from typing import Any, Mapping, NewType, Sequence

from tortoise import Tortoise, connections
from tortoise.transactions import in_transaction
from tortoise.fields import (
    BigIntField,
    BooleanField,
    CASCADE,
    IntEnumField,
    CharEnumField,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    JSONField,
    IntField,
    RESTRICT,
    SET_NULL,
    TextField,
)
from tortoise.models import Model

from katalog.config import config_file
from katalog.metadata import (
    METADATA_REGISTRY,
    METADATA_REGISTRY_BY_ID,
    MetadataDef,
    MetadataKey,
    MetadataScalar,
    ensure_value_type,
    get_metadata_def,
    get_metadata_def_by_registry_id,
    get_metadata_registry_id,
)
from katalog.utils.utils import _decode_cursor, _encode_cursor

"""Data usage notes
- The target profile for this system is to handle metadata for 1 million files. Actual file contents is not to be stored in the DB.
- This implies
- ~1 million Asset records
- ~30 million Metadata records (assuming an average of 30 metadata entries per asset). 
Metadata will mostly be shorter text and date values, but some fields may grow pretty large, such as text contents, summaries, etc.
- 10 to 100 Providers
- As data changes over time, snapshots will be created, increasing the number of Metadata rows per asset. 
On the other hand, users will be encouraged to purge snapshots regularly.
"""


def fqn(cls: type) -> str:
    # return f"{cls.__module__}.{cls.__qualname__}"
    return f"models.{cls.__qualname__}"


class OpStatus(Enum):
    IN_PROGRESS = "in_progress"
    PARTIAL = "partial"
    COMPLETED = "completed"
    CANCELED = "canceled"
    SKIPPED = "skipped"
    ERROR = "error"


class ProviderType(IntEnum):
    SOURCE = 0
    PROCESSOR = 1
    ANALYZER = 2
    EDITOR = 3
    EXPORTER = 4


class Provider(Model):
    id = IntField(pk=True)
    name = CharField(max_length=255, unique=True)
    plugin_id = CharField(max_length=255, null=True)
    class_path = CharField(max_length=1024, null=True)
    config = JSONField(null=True)
    type = IntEnumField(ProviderType)
    created_at = DatetimeField(auto_now_add=True)
    updated_at = DatetimeField(auto_now=True)

    @classmethod
    async def sync_db(cls, id: int, name: str) -> None:
        for section, ptype in (
            ("sources", ProviderType.SOURCE),
            ("processors", ProviderType.PROCESSOR),
            ("analyzers", ProviderType.ANALYZER),
        ):
            for entry in (config_file or {}).get(section, []) or []:
                entry_name = entry.get("name") or entry.get("class_path")
                if not entry_name:
                    continue
                if await cls.get_or_none(name=entry_name):
                    continue
                await cls.create(
                    name=entry_name,
                    type=ptype,
                    plugin_id=entry.get("plugin_id"),
                    class_path=entry.get("class_path"),
                    config=dict(entry),
                )


class Snapshot(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(
        fqn(Provider), related_name="snapshots", on_delete=CASCADE
    )
    started_at = DatetimeField(default=lambda: datetime.now(UTC))
    completed_at = DatetimeField(null=True)
    status = CharEnumField(OpStatus, max_length=32)
    metadata = JSONField(null=True)

    @classmethod
    async def begin(
        cls,
        provider: Provider | int,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        metadata: Mapping[str, Any] | None = None,
    ) -> "Snapshot":
        provider_id = provider.id if isinstance(provider, Provider) else provider
        return await cls.create(
            provider_id=provider_id,
            status=status,
            metadata=dict(metadata) if metadata else None,
        )

    async def finalize(
        self, *, status: OpStatus, stats: SnapshotStats | None = None
    ) -> None:
        completed_at = datetime.now(UTC)
        provider_id = self.provider
        metadata_payload: dict[str, Any] | None = None
        if stats is not None or self.metadata is not None:
            metadata_payload = dict(self.metadata or {})
            if stats is not None:
                metadata_payload["stats"] = stats.to_dict()
            self.metadata = metadata_payload

        async with in_transaction():
            update_fields = ["completed_at", "status"]
            if metadata_payload is not None:
                update_fields.append("metadata")
            self.status = status
            self.completed_at = completed_at
            await self.save(update_fields=update_fields)

            await Asset.filter(
                provider_id=provider_id,
                deleted_snapshot_id__isnull=True,
                last_snapshot_id__lt=self.id,
            ).update(deleted_snapshot_id=self.id)

            await Provider.filter(id=provider_id).update(updated_at=completed_at)

    class Meta(Model.Meta):
        indexes = (("provider", "started_at"),)


class FileAccessor(ABC):
    @abstractmethod
    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        """Fetch up to `length` bytes starting at `offset`."""


class Asset(Model):
    id = IntField(pk=True)
    provider = ForeignKeyField(fqn(Provider), related_name="assets", on_delete=CASCADE)
    canonical_id = CharField(max_length=255, unique=True)
    canonical_uri = CharField(max_length=1024, unique=True)
    created_snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="created_assets", on_delete=RESTRICT
    )
    last_snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="last_assets", on_delete=RESTRICT
    )
    deleted_snapshot = ForeignKeyField(
        fqn(Snapshot),
        related_name="deleted_assets",
        null=True,
        on_delete=SET_NULL,
    )
    _data_accessor: FileAccessor | None = None

    @property
    def data(self) -> FileAccessor | None:
        return self._data_accessor

    def attach_accessor(self, accessor: FileAccessor | None) -> None:
        self._data_accessor = accessor

    async def upsert(*args, **kwargs):
        raise NotImplementedError()

    class Meta(Model.Meta):
        indexes = (("provider", "last_snapshot"),)


class MetadataType(IntEnum):
    STRING = 0
    INT = 1
    FLOAT = 2
    DATETIME = 3
    JSON = 4
    RELATION = 5


class MetadataRegistry(Model):
    id = IntField(pk=True)
    # Owner/defining plugin id (same identifier format as Provider.plugin_id)
    plugin_id = CharField(max_length=255)
    # Canonical, globally unique key string (recommended: namespaced, e.g. "plugin_id:key").
    key = CharField(max_length=512)
    value_type = IntEnumField(MetadataType)
    title = CharField(max_length=255, default="")
    description = TextField(default="")
    width = IntField(null=True)

    class Meta(Model.Meta):
        unique_together = ("plugin_id", "key")


class Metadata(Model):
    id = IntField(pk=True)
    asset = ForeignKeyField(fqn(Asset), related_name="metadata", on_delete=CASCADE)
    provider = ForeignKeyField(
        fqn(Provider), related_name="metadata_entries", on_delete=CASCADE
    )
    snapshot = ForeignKeyField(
        fqn(Snapshot), related_name="metadata_entries", on_delete=CASCADE
    )
    metadata_key = ForeignKeyField(
        fqn(MetadataRegistry), related_name="metadata_entries", on_delete=RESTRICT
    )
    value_type = IntEnumField(MetadataType)
    value_text = TextField(null=True)
    value_int = BigIntField(null=True)
    value_real = FloatField(null=True)
    value_datetime = DatetimeField(null=True)
    value_json = JSONField(null=True)
    value_relation = ForeignKeyField(fqn(Asset), null=True, on_delete=CASCADE)
    removed = BooleanField(default=False)
    # Null means no confidence score, which can be assumed to be 1.0
    confidence = FloatField(null=True)

    class Meta(Model.Meta):
        indexes = ("metadata_key", "value_type")
        # unique_together = ("asset", "provider", "snapshot", "metadata_key")

    @property
    def key(self) -> "MetadataKey":
        """Metadata key as the typed `MetadataKey` (no DB fetch).

        Uses the startup-synced in-memory registry mapping from integer id -> key.
        """

        registry_id = getattr(self, "metadata_key_id", None)
        if registry_id is None:
            raise RuntimeError("metadata_key_id is missing on this Metadata instance")
        return get_metadata_def_by_registry_id(int(registry_id)).key

    @property
    def value(self) -> "MetadataScalar":
        """Return the stored value as a Python scalar (no DB fetch)."""

        # Prefer the declared type for speed/clarity.
        if self.value_type == MetadataType.STRING:
            return self.value_text
        if self.value_type == MetadataType.INT:
            return self.value_int
        if self.value_type == MetadataType.FLOAT:
            return self.value_real
        if self.value_type == MetadataType.DATETIME:
            return self.value_datetime
        if self.value_type == MetadataType.JSON:
            return self.value_json
        if self.value_type == MetadataType.RELATION:
            return self.value_relation
        else:
            raise ValueError(f"Unsupported metadata value_type {self.value_type}")

    @classmethod
    async def for_asset(
        cls,
        asset: Asset | int,
        *,
        include_removed: bool = False,
    ) -> Sequence["Metadata"]:
        asset_id = asset.id if isinstance(asset, Asset) else int(asset)
        query = cls.filter(asset_id=asset_id)
        if not include_removed:
            query = query.filter(removed=False)
        return await query.order_by("metadata_key_id", "id")


def make_metadata(*args, **kwargs) -> Metadata:
    """Create a Metadata instance, ensuring the value type matches the key definition."""
    # Signature is intentionally flexible because call sites differ (sources/processors/tests).
    # Preferred usage:
    #   make_metadata(provider_id, key, value, asset_id=..., snapshot_id=...)
    provider_id: int | None = None
    key: MetadataKey | None = None
    value: MetadataScalar | None = None

    if len(args) >= 1:
        provider_id = args[0]
    if len(args) >= 2:
        key = args[1]
    if len(args) >= 3:
        value = args[2]
    if len(args) > 3:
        raise TypeError(
            "make_metadata(provider_id, key, value, ...) takes at most 3 positional arguments"
        )

    provider_id = kwargs.pop("provider_id", provider_id)
    key = kwargs.pop("key", key)
    value = kwargs.pop("value", value)

    asset = kwargs.pop("asset", None)
    asset_id = kwargs.pop("asset_id", None)
    snapshot = kwargs.pop("snapshot", None)
    snapshot_id = kwargs.pop("snapshot_id", None)
    removed = kwargs.pop("removed", False)
    confidence = kwargs.pop("confidence", None)

    if kwargs:
        unknown = ", ".join(sorted(kwargs.keys()))
        raise TypeError(f"Unknown make_metadata() kwargs: {unknown}")

    if provider_id is None:
        raise TypeError("make_metadata requires provider_id")
    if key is None:
        raise TypeError("make_metadata requires key")
    if value is None:
        raise TypeError("make_metadata requires value")

    definition = get_metadata_def(key)
    ensure_value_type(definition.value_type, value)

    entry = Metadata(
        provider_id=int(provider_id),
        metadata_key_id=get_metadata_registry_id(key),
        value_type=definition.value_type,
        removed=bool(removed),
        confidence=confidence,
    )
    if asset is not None:
        entry.asset = asset
    elif asset_id is not None:
        entry.asset_id = int(asset_id)

    if snapshot is not None:
        entry.snapshot = snapshot
    elif snapshot_id is not None:
        entry.snapshot_id = int(snapshot_id)

    if definition.value_type == MetadataType.STRING:
        entry.value_text = str(value)
    elif definition.value_type == MetadataType.INT:
        # bool is rejected by _ensure_value_type
        entry.value_int = int(value)  # type: ignore[arg-type]
    elif definition.value_type == MetadataType.FLOAT:
        entry.value_real = float(value)  # type: ignore[arg-type]
    elif definition.value_type == MetadataType.DATETIME:
        entry.value_datetime = value  # type: ignore[assignment]
    elif definition.value_type == MetadataType.JSON:
        entry.value_json = value  # type: ignore[assignment]
    else:  # pragma: no cover
        raise ValueError(f"Unsupported metadata value type {definition.value_type}")

    return entry


async def list_assets_with_metadata(
    *,
    provider_id: int | None = None,
    limit: int = 100,
    cursor: str | None = None,
    order_by: str = "id",
    order_dir: str = "asc",
    include_deleted: bool = False,
    include_removed_metadata: bool = False,
    metadata_filters: Sequence[Mapping[str, Any]] | None = None,
    relationship_filters: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """List assets with their metadata for UI consumption.

    Returns one JSON object per asset (Asset fields at root), plus a `metadata`
    property containing a list of Metadata rows.

    Minimal raw-SQL implementation (avoids ORM row hydration):
    - fetch one page of assets with keyset pagination
    - fetch all metadata for those asset ids
    - stitch results in Python
    """

    if limit <= 0:
        limit = 1
    if limit > 1000:
        limit = 1000

    direction = order_dir.lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("order_dir must be 'asc' or 'desc'")

    allowed_order_fields = {
        "id",
        "canonical_id",
        "canonical_uri",
        "created_snapshot_id",
        "last_snapshot_id",
        "deleted_snapshot_id",
        "provider_id",
    }
    if order_by not in allowed_order_fields:
        raise ValueError(f"Unsupported order_by={order_by!s}")

    conn = connections.get("default")

    asset_cols = [
        "id",
        "provider_id",
        "canonical_id",
        "canonical_uri",
        "created_snapshot_id",
        "last_snapshot_id",
        "deleted_snapshot_id",
    ]

    meta_cols = [
        "id",
        "asset_id",
        "provider_id",
        "snapshot_id",
        "metadata_key_id",
        "value_type",
        "value_text",
        "value_int",
        "value_real",
        "value_datetime",
        "value_json",
        "confidence",
        "removed",
    ]

    def _metadata_value_selector(value_type: MetadataType) -> tuple[str, int]:
        if value_type == MetadataType.STRING:
            return "value_text", int(MetadataType.STRING)
        if value_type == MetadataType.INT:
            return "value_int", int(MetadataType.INT)
        if value_type == MetadataType.FLOAT:
            return "value_real", int(MetadataType.FLOAT)
        if value_type == MetadataType.DATETIME:
            return "value_datetime", int(MetadataType.DATETIME)
        if value_type == MetadataType.JSON:
            return "value_json", int(MetadataType.JSON)
        raise ValueError(f"Unsupported metadata value_type={value_type!s}")

    async def _resolve_metadata_key_id(
        *,
        key: str,
        plugin_id: str | None,
    ) -> int | None:
        sql = "SELECT id FROM metadataregistry WHERE key = ?"
        params: list[Any] = [key]
        if plugin_id is not None:
            sql += " AND plugin_id = ?"
            params.append(plugin_id)
        sql += " LIMIT 1"
        rows = await conn.execute_query_dict(sql, params)
        if not rows:
            return None
        return int(rows[0]["id"])

    where_parts: list[str] = ["1=1"]
    params: list[Any] = []

    if provider_id is not None:
        where_parts.append("a.provider_id = ?")
        params.append(int(provider_id))
    if not include_deleted:
        where_parts.append("a.deleted_snapshot_id IS NULL")

    # Keyset pagination.
    if cursor:
        state = _decode_cursor(cursor)
        if "id" not in state or "v" not in state:
            raise ValueError("cursor must contain 'v' and 'id'")
        last_id = int(state["id"])
        last_v = state["v"]
        if order_by == "id":
            where_parts.append("a.id > ?" if direction == "asc" else "a.id < ?")
            params.append(last_id)
        else:
            op = ">" if direction == "asc" else "<"
            where_parts.append(
                f"(a.{order_by} {op} ? OR (a.{order_by} = ? AND a.id {op} ?))"
            )
            params.extend([last_v, last_v, last_id])

    # Metadata filters (AND semantics across filters).
    if metadata_filters:
        for idx, raw in enumerate(metadata_filters):
            if not isinstance(raw, Mapping):
                raise ValueError(f"metadata_filters[{idx}] must be an object")
            key = raw.get("key")
            if not isinstance(key, str) or not key:
                raise ValueError(
                    f"metadata_filters[{idx}].key must be a non-empty string"
                )
            plugin_id = raw.get("plugin_id")
            if plugin_id is not None and (
                not isinstance(plugin_id, str) or not plugin_id
            ):
                raise ValueError(
                    f"metadata_filters[{idx}].plugin_id must be a non-empty string"
                )

            op = raw.get("op", "eq")
            if op != "eq":
                raise ValueError(
                    f"metadata_filters[{idx}].op only supports 'eq' for now"
                )

            if "value" not in raw:
                raise ValueError(f"metadata_filters[{idx}].value is required")
            value = raw.get("value")

            # Minimal type support: string/int/float/bool only.
            if isinstance(value, bool):
                inferred_type = MetadataType.INT
                value = int(value)
            elif isinstance(value, int):
                inferred_type = MetadataType.INT
            elif isinstance(value, float):
                inferred_type = MetadataType.FLOAT
            elif isinstance(value, str):
                inferred_type = MetadataType.STRING
            else:
                raise ValueError(
                    f"metadata_filters[{idx}].value must be string/int/float/bool for now"
                )

            value_type_raw = raw.get("value_type")
            if value_type_raw is None:
                value_type = inferred_type
            elif isinstance(value_type_raw, int):
                value_type = MetadataType(int(value_type_raw))
            elif isinstance(value_type_raw, str):
                normalized = value_type_raw.strip().lower()
                mapping = {
                    "string": MetadataType.STRING,
                    "text": MetadataType.STRING,
                    "int": MetadataType.INT,
                    "integer": MetadataType.INT,
                    "float": MetadataType.FLOAT,
                    "real": MetadataType.FLOAT,
                    "datetime": MetadataType.DATETIME,
                    "json": MetadataType.JSON,
                }
                if normalized not in mapping:
                    raise ValueError(
                        f"metadata_filters[{idx}].value_type must be one of {sorted(mapping.keys())}"
                    )
                value_type = mapping[normalized]
            else:
                raise ValueError(
                    f"metadata_filters[{idx}].value_type must be a string or int if provided"
                )

            key_id = await _resolve_metadata_key_id(key=key, plugin_id=plugin_id)
            if key_id is None:
                return {"records": [], "next_cursor": None}

            col, vt_int = _metadata_value_selector(value_type)
            exists_parts = [
                "SELECT 1 FROM metadata m",
                "WHERE m.asset_id = a.id",
                "AND m.metadata_key_id = ?",
                "AND m.value_type = ?",
                f"AND m.{col} = ?",
            ]
            exists_params: list[Any] = [key_id, vt_int, value]
            if not include_removed_metadata:
                exists_parts.insert(3, "AND m.removed = 0")
            where_parts.append("EXISTS (" + " ".join(exists_parts) + ")")
            params.extend(exists_params)

    # Relationship filters (AND semantics across filters).
    if relationship_filters:
        for idx, raw in enumerate(relationship_filters):
            if not isinstance(raw, Mapping):
                raise ValueError(f"relationship_filters[{idx}] must be an object")
            relationship_type = raw.get("relationship_type")
            if not isinstance(relationship_type, str) or not relationship_type:
                raise ValueError(
                    f"relationship_filters[{idx}].relationship_type must be a non-empty string"
                )
            direction_raw = raw.get("direction", "outgoing")
            if not isinstance(direction_raw, str):
                raise ValueError(
                    f"relationship_filters[{idx}].direction must be a string"
                )
            direction_norm = direction_raw.strip().lower()
            if direction_norm not in {"outgoing", "incoming"}:
                raise ValueError(
                    f"relationship_filters[{idx}].direction must be 'outgoing' or 'incoming'"
                )

            peer_asset_id = raw.get("peer_asset_id")
            if peer_asset_id is not None and not isinstance(peer_asset_id, int):
                raise ValueError(
                    f"relationship_filters[{idx}].peer_asset_id must be an int if provided"
                )

            rel_provider_id = raw.get("provider_id")
            if rel_provider_id is not None and not isinstance(rel_provider_id, int):
                raise ValueError(
                    f"relationship_filters[{idx}].provider_id must be an int if provided"
                )

            if direction_norm == "outgoing":
                anchor_col = "from_asset_id"
                peer_col = "to_asset_id"
            else:
                anchor_col = "to_asset_id"
                peer_col = "from_asset_id"

            exists_sql = [
                "SELECT 1 FROM assetrelationship r",
                f"WHERE r.{anchor_col} = a.id",
                "AND r.relationship_type = ?",
                "AND r.removed = 0",
            ]
            exists_params = [relationship_type]
            if rel_provider_id is not None:
                exists_sql.append("AND r.provider_id = ?")
                exists_params.append(int(rel_provider_id))
            if peer_asset_id is not None:
                exists_sql.append(f"AND r.{peer_col} = ?")
                exists_params.append(int(peer_asset_id))

            where_parts.append("EXISTS (" + " ".join(exists_sql) + ")")
            params.extend(exists_params)

    order_prefix = "" if direction == "asc" else "DESC"
    if order_by == "id":
        order_sql = f"a.id {order_prefix}".strip()
    else:
        order_sql = f"a.{order_by} {order_prefix}, a.id {order_prefix}".strip()

    asset_select = ", ".join([f"a.{c}" for c in asset_cols])
    sql = (
        f"SELECT {asset_select} "
        "FROM asset a "
        f"WHERE {' AND '.join(where_parts)} "
        f"ORDER BY {order_sql} "
        "LIMIT ?"
    )
    asset_rows = await conn.execute_query_dict(sql, [*params, int(limit) + 1])
    has_more = len(asset_rows) > limit
    asset_rows = asset_rows[:limit]

    if not asset_rows:
        return {"records": [], "next_cursor": None}

    asset_ids = [int(r["id"]) for r in asset_rows]
    placeholders = ",".join(["?"] * len(asset_ids))
    meta_select = ", ".join(meta_cols)
    meta_sql = f"SELECT {meta_select} FROM metadata WHERE asset_id IN ({placeholders}) "
    meta_params: list[Any] = list(asset_ids)
    if not include_removed_metadata:
        meta_sql += " AND removed = 0"
    meta_sql += " ORDER BY asset_id, metadata_key_id, id"
    meta_rows = await conn.execute_query_dict(meta_sql, meta_params)

    meta_by_asset: dict[int, list[dict[str, Any]]] = {aid: [] for aid in asset_ids}
    for m in meta_rows:
        key_id = m.get("metadata_key_id")
        if key_id is not None:
            try:
                m["key"] = str(get_metadata_def_by_registry_id(int(key_id)).key)
            except KeyError:
                # Registry may be out of date for this process; keep id-only.
                pass
        meta_by_asset[int(m["asset_id"])].append(m)

    records: list[dict[str, Any]] = []
    for asset_row in asset_rows:
        aid = int(asset_row["id"])
        asset_row["metadata"] = meta_by_asset.get(aid, [])
        records.append(asset_row)

    next_cursor: str | None = None
    if has_more:
        last = records[-1]
        next_cursor = _encode_cursor({"v": last.get(order_by), "id": last["id"]})

    return {
        "records": records,
        "next_cursor": next_cursor,
    }


async def setup(db_path: Path) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_url = f"sqlite://{db_path}"
    # When executed via `python -m katalog.models_tortoise`, Python runs the code as
    # `__main__`, so the model classes live in that module.
    await Tortoise.init(db_url=db_url, modules={"models": [__name__]})
    await Tortoise.generate_schemas()
    await sync_metadata_registry()
    return db_path


async def sync_metadata_registry() -> None:
    """Ensure DB registry contains the import-time declared keys.

    DB remains the source of truth: this only INSERTs missing keys; it does not
    update existing rows.
    """

    # Insert missing rows, then record the integer IDs locally for fast queries.
    for meta_key, definition in list(METADATA_REGISTRY.items()):
        row, _created = await MetadataRegistry.get_or_create(
            plugin_id=definition.plugin_id,
            key=str(definition.key),
            defaults={
                "value_type": definition.value_type,
                "title": definition.title,
                "description": definition.description,
                "width": definition.width,
            },
        )
        METADATA_REGISTRY[meta_key] = MetadataDef(
            plugin_id=definition.plugin_id,
            key=definition.key,
            registry_id=row.id,
            value_type=definition.value_type,
            title=definition.title,
            description=definition.description,
            width=definition.width,
        )

    # Rebuild the reverse mapping (id -> definition) for O(1) lookups.
    # Read the DB registry once at startup so `Metadata.key` never needs to touch
    # `MetadataRegistry` at runtime (and so it still works for keys that exist in
    # the DB but weren't imported/defined in this process).
    METADATA_REGISTRY_BY_ID.clear()
    for row in await MetadataRegistry.all():
        METADATA_REGISTRY_BY_ID[int(row.id)] = MetadataDef(
            plugin_id=row.plugin_id,
            key=MetadataKey(row.key),
            registry_id=int(row.id),
            value_type=row.value_type,
            title=row.title,
            description=row.description,
            width=row.width,
        )


@dataclass(slots=True)
class SnapshotStats:
    assets_seen: int = 0
    assets_changed: int = 0
    assets_added: int = 0
    assets_modified: int = 0
    assets_deleted: int = 0
    assets_ignored: int = 0
    assets_processed: int = 0

    metadata_values_affected: int = 0
    metadata_values_added: int = 0
    metadata_values_removed: int = 0

    relations_affected: int = 0
    relations_added: int = 0
    relations_removed: int = 0

    processings_started: int = 0
    processings_completed: int = 0
    processings_partial: int = 0
    processings_cancelled: int = 0
    processings_skipped: int = 0
    processings_error: int = 0

    _changed_assets: set[int] = field(default_factory=set, init=False, repr=False)
    _added_assets: set[int] = field(default_factory=set, init=False, repr=False)
    _modified_assets: set[int] = field(default_factory=set, init=False, repr=False)

    def record_asset_change(self, asset_id: int, *, added: bool) -> None:
        if added:
            if asset_id not in self._added_assets:
                self.assets_added += 1
                self._added_assets.add(asset_id)
        else:
            if (
                asset_id not in self._added_assets
                and asset_id not in self._modified_assets
            ):
                self.assets_modified += 1
                self._modified_assets.add(asset_id)
        if asset_id not in self._changed_assets:
            self.assets_changed += 1
            self._changed_assets.add(asset_id)

    def record_metadata_diff(self, added: int, removed: int) -> None:
        if not added and not removed:
            return
        self.metadata_values_added += added
        self.metadata_values_removed += removed
        self.metadata_values_affected += added + removed

    def record_relationship_diff(self, added: int, removed: int) -> None:
        if not added and not removed:
            return
        self.relations_added += added
        self.relations_removed += removed
        self.relations_affected += added + removed

    def to_dict(self) -> dict[str, Any]:
        assets_not_changed = max(
            self.assets_seen - self.assets_changed - self.assets_ignored, 0
        )
        assets_not_processed = max(
            self.assets_seen - self.assets_processed - self.assets_ignored, 0
        )
        return {
            "assets": {
                "seen": self.assets_seen,
                "changed": {
                    "total": self.assets_changed,
                    "added": self.assets_added,
                    "modified": self.assets_modified,
                    "deleted": self.assets_deleted,
                },
                "not_changed": assets_not_changed,
                "ignored": self.assets_ignored,
                "processed": {
                    "processed": self.assets_processed,
                    "not_processed": assets_not_processed,
                },
            },
            "metadata": {
                "values_affected": self.metadata_values_affected,
                "added": self.metadata_values_added,
                "removed": self.metadata_values_removed,
            },
            "relationships": {
                "affected": self.relations_affected,
                "added": self.relations_added,
                "removed": self.relations_removed,
            },
            "processors": {
                "started": self.processings_started,
                "completed": self.processings_completed,
                "partial": self.processings_partial,
                "cancelled": self.processings_cancelled,
                "skipped": self.processings_skipped,
                "error": self.processings_error,
            },
        }
