from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Iterable, Sequence, TYPE_CHECKING, TypeVar, overload

from pydantic import BaseModel, ConfigDict, PrivateAttr, computed_field, field_serializer

from katalog.constants.metadata import (
    MetadataDef,
    MetadataKey,
    MetadataScalar,
    MetadataType,
    get_metadata_def_by_id,
    get_metadata_def_by_key,
    get_metadata_id,
)
from katalog.models.core import Changeset
from katalog.models.assets import Asset

if TYPE_CHECKING:
    from katalog.models.core import Changeset

T = TypeVar("T")


class MetadataRegistry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    plugin_id: str
    key: str
    value_type: MetadataType
    title: str = ""
    description: str = ""
    width: int | None = None


class Metadata(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    asset_id: int | None = None
    actor_id: int | None = None
    changeset_id: int | None = None
    metadata_key_id: int | None = None

    value_type: MetadataType
    value_text: str | None = None
    value_int: int | None = None
    value_real: float | None = None
    value_datetime: datetime | None = None
    value_json: Any | None = None
    value_relation_id: int | None = None
    value_collection_id: int | None = None
    value_relation: Any | None = None
    value_collection: Any | None = None
    removed: bool = False
    confidence: float | None = None

    @computed_field(return_type=MetadataKey)
    @property
    def key(self) -> MetadataKey:
        """Metadata key as the typed MetadataKey (no DB fetch)."""

        registry_id = getattr(self, "metadata_key_id", None)
        if registry_id is None:
            raise RuntimeError("metadata_key_id is missing on this Metadata instance")
        return get_metadata_def_by_id(int(registry_id)).key

    @field_serializer("key")
    def _serialize_key(self, value: MetadataKey) -> str:
        return str(value)

    @computed_field(return_type=MetadataScalar)
    @property
    def value(self) -> "MetadataScalar":
        """Return the stored value as a Python scalar (no DB fetch)."""

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
            return self.value_relation_id
        if self.value_type == MetadataType.COLLECTION:
            return self.value_collection_id
        raise ValueError(f"Unsupported metadata value_type {self.value_type}")

    @field_serializer("value_type")
    def _serialize_value_type(self, value: MetadataType) -> str:
        return value.name if isinstance(value, MetadataType) else str(value)

    def set_value(self, value: Any) -> None:
        if value is None:
            self.value_text = None
            self.value_int = None
            self.value_real = None
            self.value_datetime = None
            self.value_json = None
            self.value_relation_id = None
            self.value_collection_id = None
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
                    self._stable_json_dumps(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"Value for JSON metadata must be JSON-serializable, got {type(value)}: {exc}"
                    ) from exc
            self.value_json = value
        elif self.value_type == MetadataType.RELATION:
            self.value_relation_id = self._coerce_fk_value(value)
            self.value_relation = value
        elif self.value_type == MetadataType.COLLECTION:
            self.value_collection_id = self._coerce_fk_value(value)
            self.value_collection = value
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
            relation_id = self.value_relation_id
            if relation_id is not None:
                return int(relation_id)
            if self.value_relation is not None:
                value_id = getattr(self.value_relation, "id", None)
                if value_id is not None:
                    return int(value_id)
                try:
                    return int(self.value_relation)
                except (TypeError, ValueError):
                    return None
            if value is None:
                return None
            value_id = getattr(value, "id", None)
            if value_id is not None:
                return int(value_id)
            return int(value)

        if self.value_type == MetadataType.COLLECTION:
            collection_id = self.value_collection_id
            if collection_id is not None:
                return int(collection_id)
            if self.value_collection is not None:
                value_id = getattr(self.value_collection, "id", None)
                if value_id is not None:
                    return int(value_id)
                try:
                    return int(self.value_collection)
                except (TypeError, ValueError):
                    return None
            if value is None:
                return None
            value_id = getattr(value, "id", None)
            if value_id is not None:
                return int(value_id)
            return int(value)

        return value


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
    if asset is not None and asset.id is not None:
        md.asset_id = asset.id
    elif asset_id is not None:
        md.asset_id = asset_id
    if changeset is not None:
        md.changeset_id = changeset.id
    elif changeset_id is not None:
        md.changeset_id = changeset_id

    return md


class MetadataChanges(BaseModel):
    """Track metadata state for an asset during processing (loaded + staged changes)."""

    asset: Asset | None = None
    loaded: Sequence[Metadata]
    staged: Sequence[Metadata] | None = None

    _loaded: list[Metadata] = PrivateAttr(default_factory=list)
    _staged: list[Metadata] = PrivateAttr(default_factory=list)
    _cache_current: dict[int | None, dict[MetadataKey, list[Metadata]]] = PrivateAttr(
        default_factory=dict
    )
    _cache_changed: dict[int | None, set[MetadataKey]] = PrivateAttr(
        default_factory=dict
    )
    _cache_latest_by_key: dict[MetadataKey, int] = PrivateAttr(default_factory=dict)
    _cache_latest_by_actor_key: dict[int, dict[MetadataKey, int]] = PrivateAttr(
        default_factory=dict
    )
    _cache_latest_ready: bool = PrivateAttr(default=False)

    def model_post_init(self, __context: Any) -> None:
        self._loaded = list(self.loaded)
        self._staged = list(self.staged or [])
        self._cache_current = {}
        self._cache_changed = {}
        self._cache_latest_by_key = {}
        self._cache_latest_by_actor_key = {}
        self._cache_latest_ready = False

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
            key=lambda m: m.changeset_id if m.changeset_id is not None else 0,
            reverse=True,
        )

        result: dict[MetadataKey, list[Metadata]] = {}
        seen_values: dict[MetadataKey, set[Any]] = {}
        for entry in ordered:
            if actor_id is not None:
                entry_actor_id = entry.actor_id
                if entry_actor_id is None or int(entry_actor_id) != int(actor_id):
                    continue
            key = entry.key
            seen_for_key = seen_values.setdefault(key, set())
            value_key = entry.fingerprint()
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
        self._cache_latest_by_key.clear()
        self._cache_latest_by_actor_key.clear()
        self._cache_latest_ready = False

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

    def entries_for_key(
        self, key: MetadataKey, actor_id: int | None = None
    ) -> list[Metadata]:
        """Return current metadata entries for a key (optionally filtered by actor)."""
        return list(self.current(actor_id).get(key, []))

    def values_for_key(
        self, key: MetadataKey, actor_id: int | None = None
    ) -> list[MetadataScalar]:
        """Return current scalar values for a key (optionally filtered by actor)."""
        return [entry.value for entry in self.entries_for_key(key, actor_id)]

    @overload
    def latest_value(
        self,
        key: MetadataKey,
        actor_id: int | None = None,
        *,
        value_type: None = None,
    ) -> MetadataScalar | None: ...

    @overload
    def latest_value(
        self,
        key: MetadataKey,
        actor_id: int | None = None,
        *,
        value_type: type[T],
    ) -> T | None: ...

    def latest_value(
        self,
        key: MetadataKey,
        actor_id: int | None = None,
        *,
        value_type: type[Any] | None = None,
    ) -> Any:
        """Return the latest current value for a key, optionally constrained by Python type."""
        entries = self.entries_for_key(key, actor_id)
        if not entries:
            return None
        value = entries[0].value
        if value_type is None:
            return value
        if isinstance(value, value_type):
            return value
        return None

    def latest_changeset_id(
        self, keys: set[MetadataKey], actor_id: int | None = None
    ) -> int | None:
        """Return the latest changeset id for the given keys (optionally filtered by actor)."""
        if not keys:
            return None
        self._ensure_latest_cache()
        if actor_id is None:
            latest = max(
                (self._cache_latest_by_key.get(key, 0) for key in keys), default=0
            )
            return latest or None
        actor_cache = self._cache_latest_by_actor_key.get(int(actor_id))
        if not actor_cache:
            return None
        latest = max((actor_cache.get(key, 0) for key in keys), default=0)
        return latest or None

    def changed_since_actor(
        self,
        keys: set[MetadataKey],
        *,
        actor_id: int,
        actor_outputs: set[MetadataKey],
    ) -> bool:
        """Return True if any of the keys changed since the actor last wrote outputs.

        NOTE: "last run" is inferred from the latest changeset id of any output key for the actor.
        If we later want stricter behavior (e.g. per-key output tracking), update this logic.
        """
        last_run = self.latest_changeset_id(actor_outputs, actor_id=actor_id)
        if last_run is None:
            return True
        latest_dep = self.latest_changeset_id(keys)
        if latest_dep is None:
            return False
        return latest_dep > last_run

    def _ensure_latest_cache(self) -> None:
        if self._cache_latest_ready:
            return
        self._cache_latest_by_key = {}
        self._cache_latest_by_actor_key = {}
        for entry in self.all_entries():
            changeset_id = entry.changeset_id
            if changeset_id is None:
                continue
            key = entry.key
            current_latest = self._cache_latest_by_key.get(key, 0)
            if changeset_id > current_latest:
                self._cache_latest_by_key[key] = int(changeset_id)
            actor_id = entry.actor_id
            if actor_id is None:
                continue
            actor_cache = self._cache_latest_by_actor_key.setdefault(int(actor_id), {})
            actor_latest = actor_cache.get(key, 0)
            if changeset_id > actor_latest:
                actor_cache[key] = int(changeset_id)
        self._cache_latest_ready = True

    def pending_entries(self) -> list[Metadata]:
        """Metadata added during processing that should be persisted."""
        return list(self._staged)

    def all_entries(self) -> list[Metadata]:
        """Loaded + staged metadata."""
        return list(self._loaded) + list(self._staged)

    def prepare_persist(
        self,
        *,
        changeset: Changeset,
        existing_metadata: Sequence["Metadata"],
    ) -> tuple[list["Metadata"], set[MetadataKey]]:
        """Compute metadata rows to persist, given current persisted metadata."""
        asset = self.asset
        if asset is None:
            raise ValueError("MetadataChanges.asset is not set for persistence")
        staged = self.pending_entries()
        if not staged:
            return [], set()

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
            metadata_key_id = entry.metadata_key_id
            if metadata_key_id is None:
                continue
            if entry.actor_id is None:
                continue
            state_key = (
                int(metadata_key_id),
                int(entry.actor_id),
                value_key,
            )
            if state_key in latest_states:
                continue
            latest_states[state_key] = bool(entry.removed)

        to_create: list[Metadata] = []
        changed_keys: set[MetadataKey] = set()

        clear_groups: set[tuple[int, int]] = set()
        skip_entries: set[int] = set()
        for md in staged:
            if md.actor_id is None:
                raise ValueError("Metadata actor_id is not set for persistence")
            if md.metadata_key_id is None:
                raise ValueError("Metadata metadata_key_id is not set for persistence")
            group_key = (int(md.metadata_key_id), int(md.actor_id))
            definition = get_metadata_def_by_id(int(md.metadata_key_id))
            if self._should_clear_on_false(definition, md):
                clear_groups.add(group_key)
                skip_entries.add(id(md))
                continue
            if self._should_skip_false(definition, md):
                skip_entries.add(id(md))
                continue
            if md.fingerprint() is None and not md.removed:
                clear_groups.add(group_key)

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
                    if existing_entry.actor_id is None:
                        continue
                    metadata_key_id = existing_entry.metadata_key_id
                    if metadata_key_id is None:
                        continue
                    if existing_entry.value_type in (
                        MetadataType.RELATION,
                        MetadataType.COLLECTION,
                    ):
                        value_key = existing_entry.fingerprint()
                        if value_key is None:
                            continue
                        state_key = (
                            int(metadata_key_id),
                            int(existing_entry.actor_id),
                            value_key,
                        )
                    else:
                        if existing_entry.fingerprint() is None:
                            continue
                        state_key = (
                            int(metadata_key_id),
                            int(existing_entry.actor_id),
                            existing_entry.fingerprint(),
                        )
                    if state_key in latest_states and latest_states[state_key]:
                        continue
                    tombstone = Metadata(
                        asset_id=asset.id,
                        actor_id=existing_entry.actor_id,
                        changeset_id=changeset.id,
                        metadata_key_id=existing_entry.metadata_key_id,
                        value_type=existing_entry.value_type,
                        removed=True,
                    )
                    tombstone.set_value(existing_entry.value)
                    to_create.append(tombstone)
                    latest_states[state_key] = True
                    changed_keys.add(existing_entry.key)

        for md in staged:
            if id(md) in skip_entries:
                continue
            if md.fingerprint() is None and not md.removed:
                continue
            if md.asset_id is None:
                md.asset_id = asset.id
            if md.changeset_id is None:
                md.changeset_id = changeset.id
            value_key = md.fingerprint()
            if value_key is None:
                continue
            if md.metadata_key_id is None or md.actor_id is None:
                raise ValueError("Metadata missing metadata_key_id or actor_id")
            state_key = (int(md.metadata_key_id), int(md.actor_id), value_key)
            existing_state = latest_states.get(state_key)
            if existing_state is not None and existing_state == bool(md.removed):
                continue
            to_create.append(md)
            latest_states[state_key] = bool(md.removed)
            changed_keys.add(md.key)

        return to_create, changed_keys

    @staticmethod
    def _should_skip_false(definition: MetadataDef, md: "Metadata") -> bool:
        if md.removed or not definition.skip_false:
            return False
        value = md.value
        return value in (0, False)

    @staticmethod
    def _should_clear_on_false(definition: MetadataDef, md: "Metadata") -> bool:
        if md.removed or not definition.clear_on_false:
            return False
        value = md.value
        return value in (0, False)


def _metadata_to_row(entry: Metadata) -> dict[str, Any]:
    value_relation_id = entry.value_relation_id
    if value_relation_id is None and entry.value_relation is not None:
        try:
            value_relation_id = int(getattr(entry.value_relation, "id", entry.value_relation))
        except (TypeError, ValueError):
            value_relation_id = None
    value_collection_id = entry.value_collection_id
    if value_collection_id is None and entry.value_collection is not None:
        try:
            value_collection_id = int(getattr(entry.value_collection, "id", entry.value_collection))
        except (TypeError, ValueError):
            value_collection_id = None
    return {
        "asset_id": entry.asset_id,
        "actor_id": entry.actor_id,
        "changeset_id": entry.changeset_id,
        "metadata_key_id": entry.metadata_key_id,
        "value_type": int(entry.value_type),
        "value_text": entry.value_text,
        "value_int": entry.value_int,
        "value_real": entry.value_real,
        "value_datetime": entry.value_datetime,
        "value_json": entry.value_json,
        "value_relation_id": value_relation_id,
        "value_collection_id": value_collection_id,
        "removed": 1 if entry.removed else 0,
        "confidence": entry.confidence,
    }


def _normalize_metadata_row(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    value_type = row.get("value_type")
    if value_type is not None and not isinstance(value_type, MetadataType):
        row["value_type"] = MetadataType(int(value_type))
    if row.get("value_type") == MetadataType.JSON:
        value_json = row.get("value_json")
        if isinstance(value_json, str):
            try:
                row["value_json"] = json.loads(value_json)
            except ValueError:
                # Keep the original string when the DB value is not encoded JSON text.
                row["value_json"] = value_json
    return row
