from __future__ import annotations

from typing import Sequence

from pydantic import BaseModel, ConfigDict, computed_field, field_serializer

from katalog.constants.metadata import (
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


class ColumnSpec(BaseModel):
    """Describe a column that can be shown in an asset view."""

    model_config = ConfigDict(frozen=True)

    id: str
    value_type: MetadataType
    registry_id: int | None
    title: str
    description: str = ""
    width: int | None = None
    hidden: bool = False
    sortable: bool = False
    filterable: bool = False
    searchable: bool = False
    plugin_id: str | None = None

    @computed_field(return_type=str)
    def key(self) -> str:
        return self.id

    @field_serializer("value_type")
    def _serialize_value_type(self, value: MetadataType) -> int:
        return int(value) if isinstance(value, MetadataType) else int(value)

    @classmethod
    def from_metadata(
        cls,
        def_key: MetadataKey,
        *,
        hidden: bool = False,
        sortable: bool = False,
        filterable: bool = False,
        searchable: bool = False,
        width: int | None = None,
    ) -> "ColumnSpec":
        definition = get_metadata_def_by_key(def_key)
        return cls(
            id=str(definition.key),
            value_type=definition.value_type,
            registry_id=definition.registry_id,
            title=definition.title or str(definition.key),
            description=definition.description,
            width=width or definition.width,
            hidden=hidden,
            sortable=sortable,
            filterable=filterable,
            searchable=searchable,
            plugin_id=definition.plugin_id,
        )


class ViewSpec(BaseModel):
    """Describe a view (set of columns + capabilities)."""

    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    columns: Sequence[ColumnSpec]
    default_sort: Sequence[tuple[str, str]] = ()
    default_columns: Sequence[str] | None = None

    def column_map(self) -> dict[str, ColumnSpec]:
        return {col.id: col for col in self.columns}


def default_view() -> ViewSpec:
    """Default view: mirrors the previous list_assets_with_metadata output."""
    columns: list[ColumnSpec] = [
        ColumnSpec.from_metadata(ASSET_ID, sortable=True, width=80),
        # ColumnSpec.from_metadata(ASSET_ACTOR_ID, sortable=True, filterable=True),
        ColumnSpec.from_metadata(ASSET_EXTERNAL_ID, searchable=True),
        ColumnSpec.from_metadata(
            ASSET_CANONICAL_URI,
            hidden=True,
            searchable=True,
        ),
        ColumnSpec.from_metadata(FILE_PATH, filterable=True, width=400),
        ColumnSpec.from_metadata(FILE_NAME, filterable=True),
        ColumnSpec.from_metadata(FILE_SIZE, filterable=True, width=80),
        ColumnSpec.from_metadata(FILE_TYPE, filterable=True),
        ColumnSpec.from_metadata(TIME_CREATED, filterable=True, width=210),
        ColumnSpec.from_metadata(TIME_MODIFIED, filterable=True, width=210),
        ColumnSpec.from_metadata(FLAG_FAVORITE, filterable=True),
        ColumnSpec.from_metadata(HASH_MD5, filterable=True, width=250),
    ]

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
