from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Sequence

from katalog.metadata import (
    ASSET_EXTERNAL_ID,
    ASSET_CANONICAL_URI,
    ASSET_ID,
    ASSET_ACTOR_ID,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    FILE_TYPE,
    FLAG_FAVORITE,
    HASH_MD5,
    METADATA_REGISTRY,
    MetadataKey,
    MetadataType,
    get_metadata_def_by_key,
    TIME_CREATED,
    TIME_MODIFIED,
)


@dataclass(frozen=True)
class ColumnSpec:
    """Describe a column that can be shown in an asset view."""

    id: str
    value_type: MetadataType
    registry_id: int | None
    title: str
    description: str = ""
    width: int | None = None
    sortable: bool = False
    filterable: bool = False
    searchable: bool = False
    plugin_id: str | None = None

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["value_type"] = int(self.value_type)
        payload["key"] = self.id
        return payload


@dataclass(frozen=True)
class ViewSpec:
    """Describe a view (set of columns + capabilities)."""

    id: str
    name: str
    columns: Sequence[ColumnSpec]
    default_sort: Sequence[tuple[str, str]] = ()
    default_columns: Sequence[str] | None = None

    def column_map(self) -> dict[str, ColumnSpec]:
        return {col.id: col for col in self.columns}

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "default_sort": list(self.default_sort),
            "default_columns": list(self.default_columns)
            if self.default_columns is not None
            else None,
        }


def _metadata_column(def_key: MetadataKey) -> ColumnSpec:
    definition = METADATA_REGISTRY[def_key]
    return ColumnSpec(
        id=str(definition.key),
        value_type=definition.value_type,
        registry_id=definition.registry_id,
        title=definition.title or str(definition.key),
        description=definition.description,
        width=definition.width,
        sortable=False,
        filterable=True,
        searchable=False,
        plugin_id=definition.plugin_id,
    )


def _asset_columns() -> Iterable[ColumnSpec]:
    for key in [
        ASSET_ID,
        ASSET_ACTOR_ID,
        ASSET_EXTERNAL_ID,
        ASSET_CANONICAL_URI,
    ]:
        definition = get_metadata_def_by_key(key)
        sortable = definition.value_type in (
            MetadataType.STRING,
            MetadataType.INT,
            MetadataType.FLOAT,
            MetadataType.DATETIME,
        )
        filterable = sortable
        yield ColumnSpec(
            id=str(definition.key),
            value_type=definition.value_type,
            registry_id=definition.registry_id,
            title=definition.title or str(definition.key),
            description=definition.description,
            width=definition.width,
            sortable=sortable,
            filterable=filterable,
            searchable=definition.value_type == MetadataType.STRING,
            plugin_id=definition.plugin_id,
        )


def default_view() -> ViewSpec:
    """Default view: mirrors the previous list_assets_with_metadata output."""

    columns: list[ColumnSpec] = list(_asset_columns())
    selected_metadata_keys = [
        FILE_PATH,
        FILE_NAME,
        FILE_SIZE,
        FILE_TYPE,
        TIME_CREATED,
        TIME_MODIFIED,
        FLAG_FAVORITE,
        HASH_MD5,
    ]
    for def_key in selected_metadata_keys:
        if def_key not in METADATA_REGISTRY:
            continue
        columns.append(_metadata_column(def_key))

    return ViewSpec(
        id="default",
        name="Default",
        columns=columns,
        default_sort=[(str(ASSET_ID), "asc")],
        default_columns=None,  # Means all columns for now.
    )


def list_views() -> list[ViewSpec]:
    return [default_view()]


def get_view(view_id: str) -> ViewSpec:
    if view_id == "default":
        return default_view()
    raise KeyError(f"Unknown view_id {view_id}")
