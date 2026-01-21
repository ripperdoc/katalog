from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import Any, Sequence, TYPE_CHECKING

from loguru import logger
from tortoise import Tortoise
from tortoise.fields import (
    CASCADE,
    BigIntField,
    BooleanField,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    ForeignKeyRelation,
    IntEnumField,
    IntField,
    JSONField,
    TextField,
    RESTRICT,
)
from tortoise.models import Model

from katalog.constants.metadata import (
    MetadataKey,
    MetadataScalar,
    MetadataType,
    get_metadata_def_by_id,
    get_metadata_def_by_key,
    get_metadata_id,
)


class MetadataRegistry(Model):
    id = IntField(pk=True)
    # Owner/defining plugin id (import path to the plugin class)
    plugin_id = CharField(max_length=1024)
    key = CharField(max_length=512)
    value_type = IntEnumField(MetadataType)
    title = CharField(max_length=255, default="")
    description = TextField(default="")
    width = IntField(null=True)

    class Meta(Model.Meta):
        # Enforce unique keys per plugin for sync_metadata_registry().
        unique_together = ("plugin_id", "key")


class Metadata(Model):
    id = IntField(pk=True)
    asset = ForeignKeyField("models.Asset", related_name="metadata", on_delete=CASCADE)
    actor: ForeignKeyRelation["Actor"] = ForeignKeyField(
        "models.Actor", related_name="metadata_entries", on_delete=CASCADE
    )
    changeset = ForeignKeyField(
        "models.Changeset", related_name="metadata_entries", on_delete=CASCADE
    )
    metadata_key = ForeignKeyField(
        "models.MetadataRegistry", related_name="metadata_entries", on_delete=RESTRICT
    )
    # Just for fixing type errors, these are populated via ForeignKeyField
    asset_id: int
    actor_id: int
    changeset_id: int
    metadata_key_id: int

    value_type = IntEnumField(MetadataType)
    value_text = TextField(null=True)
    value_int = BigIntField(null=True)
    value_real = FloatField(null=True)
    value_datetime = DatetimeField(null=True)
    value_json = JSONField(null=True)
    value_relation = ForeignKeyField("models.Asset", null=True, on_delete=CASCADE)
    value_collection = ForeignKeyField(
        "models.AssetCollection", null=True, on_delete=CASCADE
    )
    removed = BooleanField(default=False)
    # Null means no confidence score, which can be assumed to be 1.0
    confidence = FloatField(null=True)

    class Meta(Model.Meta):
        indexes = (
            # Used by list_assets_for_view(), list_grouped_assets(), and _metadata_filter_condition().
            ("asset", "metadata_key", "changeset"),
            # Used by collection membership lookups (collection/member key).
            ("metadata_key", "value_collection"),
        )
        # unique_together = ("asset", "actor", "changeset", "metadata_key")

    @property
    def key(self) -> "MetadataKey":
        """Metadata key as the typed `MetadataKey` (no DB fetch).

        Uses the startup-synced in-memory registry mapping from integer id -> key.
        """

        registry_id = getattr(self, "metadata_key_id", None)
        if registry_id is None:
            raise RuntimeError("metadata_key_id is missing on this Metadata instance")
        return get_metadata_def_by_id(int(registry_id)).key

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
        if self.value_type == MetadataType.COLLECTION:
            return self.value_collection
        else:
            raise ValueError(f"Unsupported metadata value_type {self.value_type}")

    def to_dict(self) -> dict[str, Any]:
        value: Any
        if self.value_type == MetadataType.STRING:
            value = self.value_text
        elif self.value_type == MetadataType.INT:
            value = self.value_int
        elif self.value_type == MetadataType.FLOAT:
            value = self.value_real
        elif self.value_type == MetadataType.DATETIME:
            value = self.value_datetime.isoformat() if self.value_datetime else None
        elif self.value_type == MetadataType.JSON:
            value = self.value_json
        elif self.value_type == MetadataType.RELATION:
            value = self.value_relation_id
        elif self.value_type == MetadataType.COLLECTION:
            value = self.value_collection_id
        else:
            value = None

        return {
            "id": int(self.id),
            "asset_id": int(self.asset_id),
            "actor_id": int(self.actor_id),
            "changeset_id": int(self.changeset_id),
            "metadata_key_id": int(self.metadata_key_id),
            "key": str(self.key),
            "value_type": self.value_type.name,
            "value": value,
            "removed": bool(self.removed),
            "confidence": float(self.confidence)
            if self.confidence is not None
            else None,
        }

    def set_value(self, value: Any) -> None:
        if value is None:
            self.value_text = None
            self.value_int = None
            self.value_real = None
            self.value_datetime = None
            self.value_json = None
            self.value_relation_id = None
            return
        if self.value_type == MetadataType.STRING:
            self.value_text = str(value)
        elif self.value_type == MetadataType.INT:
            self.value_int = int(value)
        elif self.value_type == MetadataType.FLOAT:
            self.value_real = float(value)
        elif self.value_type == MetadataType.DATETIME:
            if not isinstance(value, datetime):
                raise ValueError(
                    f"Expected datetime for MetadataType.DATETIME, got {type(value)}"
                )
            if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
                raise ValueError(
                    "value_datetime must be timezone-aware (e.g. UTC). "
                    "Provide an aware datetime."
                )
            self.value_datetime = value
        elif self.value_type == MetadataType.JSON:
            if value is not None:
                try:
                    # Validate that the value is actually JSON-serializable.
                    # We use a stable encoding to avoid surprising behavior across runs.
                    self._stable_json_dumps(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Value for JSON metadata must be JSON-serializable, got {type(value)}: {exc}"
                    ) from exc
            self.value_json = value
        elif self.value_type == MetadataType.RELATION:
            self.value_relation_id = self._coerce_fk_value(value)
        elif self.value_type == MetadataType.COLLECTION:
            self.value_collection_id = self._coerce_fk_value(value)
        else:
            raise ValueError(
                f"Unsupported value to set '{value}' of type '{type(value)} for Metadata of type {self.value_type}"
            )

    def __str__(self) -> str:
        return f"Metadata('{self.key}'='{self.value}', id={self.id}, actor={self.actor_id}, removed={self.removed})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def _stable_json_dumps(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @staticmethod
    def _coerce_fk_value(value: Any) -> int:
        if value is None:
            raise ValueError("Foreign key value must not be None")
        if isinstance(value, int):
            return int(value)
        value_id = getattr(value, "id", None)
        if value_id is not None:
            return int(value_id)
        return int(value)

    def fingerprint(self) -> Any:
        """Return a hashable, stable representation of this metadata value.

        Used for change detection and duplicate prevention.

        Important: for JSON values we compare a stable JSON encoding rather than Python object
        identity.
        """

        value: Any = self.value

        if self.value_type == MetadataType.JSON:
            if value is None:
                return None
            return self._stable_json_dumps(value)

        if self.value_type == MetadataType.DATETIME:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.isoformat()
            return str(value)

        if self.value_type == MetadataType.RELATION:
            relation_id = getattr(self, "value_relation_id", None)
            if relation_id is not None:
                return int(relation_id)
            relation = getattr(self, "value_relation", None)
            if relation is not None:
                relation_pk = getattr(relation, "id", None)
                if relation_pk is not None:
                    return int(relation_pk)
            if value is None:
                return None
            value_id = getattr(value, "id", None)
            if value_id is not None:
                return int(value_id)
            return int(value)

        if self.value_type == MetadataType.COLLECTION:
            collection_id = getattr(self, "value_collection_id", None)
            if collection_id is not None:
                return int(collection_id)
            collection = getattr(self, "value_collection", None)
            if collection is not None:
                collection_pk = getattr(collection, "id", None)
                if collection_pk is not None:
                    return int(collection_pk)
            if value is None:
                return None
            value_id = getattr(value, "id", None)
            if value_id is not None:
                return int(value_id)
            return int(value)

        return value

    @classmethod
    async def for_asset(
        cls,
        asset: Asset | int,
        *,
        include_removed: bool = False,
    ) -> Sequence["Metadata"]:
        from .assets import Asset

        asset_id = asset.id if isinstance(asset, Asset) else int(asset)
        query = cls.filter(asset_id=asset_id)
        if not include_removed:
            query = query.filter(removed=False)
        return await query.order_by("metadata_key_id", "id")


def make_metadata(
    key: MetadataKey,
    value: MetadataScalar | None = None,
    actor_id: int | None = None,
    removed: bool = False,
    confidence: float | None = None,
    *,
    asset: Asset | None = None,
    asset_id: int | None = None,
    changeset: Changeset | None = None,
    changeset_id: int | None = None,
    metadata_id: int | None = None,  # Only used for testing or bypassing
) -> Metadata:
    """Create a Metadata instance, ensuring the value type matches the key definition."""
    definition = get_metadata_def_by_key(key)

    md = Metadata(
        metadata_key_id=get_metadata_id(key) if metadata_id is None else metadata_id,
        value_type=definition.value_type,
        removed=removed,
        confidence=confidence,
    )
    md.set_value(value)
    if actor_id is not None:
        md.actor_id = actor_id
    if asset is not None:
        md.asset = asset
    elif asset_id is not None:
        md.asset_id = asset_id
    if changeset is not None:
        md.changeset = changeset
    elif changeset_id is not None:
        md.changeset_id = changeset_id

    return md


@dataclass(slots=True)
class MetadataChanges:
    """Track metadata state for an asset during processing (loaded + staged changes)."""

    loaded: Sequence[Metadata]
    staged: Sequence[Metadata] | None = None

    # Internal runtime fields (not part of the generated init)
    _loaded: list[Metadata] = field(init=False)
    _staged: list[Metadata] = field(init=False)
    _cache_current: dict[int | None, dict[MetadataKey, list[Metadata]]] = field(
        default_factory=dict, init=False
    )
    _cache_changed: dict[int | None, set[MetadataKey]] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        self._loaded = list(self.loaded)
        self._staged = list(self.staged or [])
        self._cache_current = {}
        self._cache_changed = {}

    @staticmethod
    def _current_metadata(
        metadata: Sequence[Metadata] | None = None,
        actor_id: int | None = None,
    ) -> dict[MetadataKey, list[Metadata]]:
        """Get current metadata entries by key from a list of Metadata."""
        if not metadata:
            return {}

        ordered = sorted(
            metadata,
            # Metadata should always have a changeset id, if not we assume 0 which
            # means "oldest"
            key=lambda m: m.changeset_id if m.changeset_id is not None else 0,
            reverse=True,
        )

        result: dict[MetadataKey, list[Metadata]] = {}
        seen_values: dict[MetadataKey, set[Any]] = {}
        for entry in ordered:
            if actor_id is not None and int(entry.actor_id) != int(actor_id):
                continue
            key = entry.key
            seen_for_key = seen_values.setdefault(key, set())
            value_key = entry.fingerprint()
            # None means "no value" and should not be treated as a stored metadata value.
            # Historically we may have persisted NULL-value rows; ignore those for the
            # purpose of computing current state.
            if value_key is None:
                continue
            if value_key in seen_for_key:
                continue
            seen_for_key.add(value_key)
            if entry.removed:
                continue
            result.setdefault(key, []).append(entry)
        return result

    def add(self, metadata: Sequence[Metadata]) -> None:
        """Stage new metadata (including removals)."""
        self._staged.extend(metadata)
        self._cache_current.clear()
        self._cache_changed.clear()

    def current(self, actor_id: int | None = None) -> dict[MetadataKey, list[Metadata]]:
        """Return current metadata by key, combining loaded and staged."""
        if actor_id in self._cache_current:
            return self._cache_current[actor_id]
        combined = list(self._loaded) + list(self._staged)
        current = self._current_metadata(combined, actor_id)
        self._cache_current[actor_id] = current
        return current

    def changed_keys(self, actor_id: int | None = None) -> set[MetadataKey]:
        """Return keys whose current values differ from the loaded baseline."""
        if actor_id in self._cache_changed:
            return self._cache_changed[actor_id]
        baseline = self._current_metadata(self._loaded, actor_id)
        current = self.current(actor_id)
        changed: set[MetadataKey] = set()
        for key in set(baseline.keys()) | set(current.keys()):
            base_values = {md.fingerprint() for md in baseline.get(key, [])}
            curr_values = {md.fingerprint() for md in current.get(key, [])}
            if base_values != curr_values:
                changed.add(key)
        self._cache_changed[actor_id] = changed
        return changed

    def has(self, key: MetadataKey, actor_id: int | None = None) -> bool:
        return key in self.current(actor_id)

    def pending_entries(self) -> list[Metadata]:
        """Metadata added during processing that should be persisted."""
        return list(self._staged)

    def all_entries(self) -> list[Metadata]:
        """Loaded + staged metadata."""
        return list(self._loaded) + list(self._staged)

    # NOTE (future refactor idea):
    # - A staged value=None means "clear_key" for that (actor_id, metadata_key) and should NOT
    #   be persisted as a NULL-value metadata row.
    # - Clear must remain append-only/undoable: we express it by writing removed=True rows for each
    #   currently-active value (per-value tombstones), not by destructive deletes.
    # - Missing keys are unchanged; only explicitly staged None triggers clear.
    # - Reads like current() intentionally hide removed rows; persistence needs latest *state* per
    #   (metadata_key_id, actor_id, value) (incl removed bit) to dedupe correctly and support
    #   add -> remove -> add over time. Ordering must be newest-first (changeset_id/id).
    # - A cleaner rewrite could factor a shared latest() helper and derive current() from it;
    #   schema-level "clear all" tombstones would reduce writes but complicate queries.
    async def persist(
        self,
        asset: "Asset",
        changeset: "Changeset",
    ) -> set[MetadataKey]:
        """Persist staged metadata entries from a change set for the given asset."""
        staged = self.pending_entries()
        if not staged:
            return set()

        existing_metadata = await asset.load_metadata()
        # Deduplication is based on latest state (not "ever seen"), so we can support:
        # add -> remove -> add (same value) across changesets.
        ordered_existing = sorted(
            existing_metadata,
            key=lambda m: (
                m.changeset_id if m.changeset_id is not None else 0,
                m.id if m.id is not None else 0,
            ),
            reverse=True,
        )
        latest_states: dict[tuple[int, int, Any], bool] = {}
        for entry in ordered_existing:
            value_key = entry.fingerprint()
            if value_key is None:
                continue
            state_key = (
                int(entry.metadata_key_id),
                int(entry.actor_id),
                value_key,
            )
            if state_key in latest_states:
                continue
            latest_states[state_key] = bool(entry.removed)

        to_create: list[Metadata] = []
        changed_keys: set[MetadataKey] = set()

        # A staged entry with value=None (and removed=False) is an explicit instruction to clear
        # all current values for (metadata_key_id, actor_id).
        clear_groups: set[tuple[int, int]] = set()
        for md in staged:
            if md.actor_id is None:
                raise ValueError("Metadata actor_id is not set for persistence")
            group_key = (int(md.metadata_key_id), int(md.actor_id))
            if md.fingerprint() is None and not md.removed:
                clear_groups.add(group_key)

        # Apply clears first.
        if clear_groups:
            existing_current_by_actor: dict[int, dict[MetadataKey, list[Metadata]]] = {}
            for metadata_key_id, actor_id in clear_groups:
                if actor_id not in existing_current_by_actor:
                    existing_current_by_actor[actor_id] = self._current_metadata(
                        existing_metadata, actor_id
                    )

                key = get_metadata_def_by_id(int(metadata_key_id)).key
                existing_current = existing_current_by_actor[actor_id].get(key, [])

                for existing_entry in existing_current:
                    if existing_entry.value_type in (
                        MetadataType.RELATION,
                        MetadataType.COLLECTION,
                    ):
                        if existing_entry.value_type == MetadataType.RELATION:
                            existing_value = getattr(
                                existing_entry, "value_relation_id", None
                            )
                        else:
                            existing_value = getattr(
                                existing_entry, "value_collection_id", None
                            )
                    else:
                        existing_value = existing_entry.value

                    if existing_value is None:
                        continue

                    removal = make_metadata(
                        key,
                        existing_value,
                        actor_id=actor_id,
                        removed=True,
                    )
                    removal.asset = asset
                    removal.asset_id = asset.id
                    removal.changeset = changeset
                    removal.changeset_id = changeset.id

                    value_key = removal.fingerprint()
                    if value_key is None:
                        continue

                    state_key = (int(metadata_key_id), int(actor_id), value_key)
                    if latest_states.get(state_key) is True:
                        continue

                    to_create.append(removal)
                    latest_states[state_key] = True
                    changed_keys.add(key)

        # Apply normal staged entries.
        for md in staged:
            md.asset = asset
            md.asset_id = asset.id
            md.changeset = changeset
            md.changeset_id = changeset.id
            if md.actor is None and md.actor_id is None:
                raise ValueError("Metadata actor_id is not set for persistence")

            value_key = md.fingerprint()

            # Never persist NULL-value rows.
            if value_key is None:
                if md.removed:
                    raise ValueError(
                        "Removal rows must include a concrete value; use value=None (removed=False) to clear all values"
                    )
                continue

            state_key = (int(md.metadata_key_id), int(md.actor_id), value_key)
            if latest_states.get(state_key) == bool(md.removed):
                continue

            to_create.append(md)
            latest_states[state_key] = bool(md.removed)
            changed_keys.add(get_metadata_def_by_id(int(md.metadata_key_id)).key)

        if to_create:
            removed = sum(1 for md in to_create if md.removed is True)
            changeset.stats.metadata_values_added += len(to_create) - removed
            changeset.stats.metadata_values_removed += removed
            changeset.stats.metadata_values_changed += len(to_create)
            await Metadata.bulk_create(to_create)
            if asset._metadata_cache is not None:
                asset._metadata_cache.extend(to_create)

            # Update full-text search index for this asset based on current metadata.
            try:
                combined = list(existing_metadata) + list(to_create)
                current = self._current_metadata(combined)
                parts: list[str] = []
                for entries in current.values():
                    for md in entries:
                        if (
                            md.value_type == MetadataType.STRING
                            and md.value_text is not None
                        ):
                            parts.append(md.value_text)
                        elif (
                            md.value_type == MetadataType.INT
                            and md.value_int is not None
                        ):
                            parts.append(str(md.value_int))
                        elif (
                            md.value_type == MetadataType.FLOAT
                            and md.value_real is not None
                        ):
                            parts.append(str(md.value_real))
                        elif (
                            md.value_type == MetadataType.JSON
                            and md.value_json is not None
                        ):
                            parts.append(json.dumps(md.value_json, ensure_ascii=False))

                doc = "\n".join(parts)
                conn = Tortoise.get_connection("default")
                # Virtual tables don't support UPSERT; replace by delete+insert.
                await conn.execute_query(
                    "DELETE FROM asset_search WHERE rowid = ?", [asset.id]
                )
                await conn.execute_query(
                    "INSERT INTO asset_search(rowid, doc) VALUES(?, ?)",
                    [asset.id, doc],
                )
            except Exception as exc:
                logger.opt(exception=exc).warning(
                    f"Failed to update asset_search index for asset_id={asset.id}"
                )
        return changed_keys


if TYPE_CHECKING:
    from .assets import Asset
    from .core import Actor, Changeset
