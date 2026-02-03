from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Pagination(BaseModel):
    offset: int
    limit: int


class QueryStats(BaseModel):
    returned: int
    total: int | None
    duration_ms: int
    duration_assets_ms: int | None = None
    duration_metadata_ms: int | None = None
    duration_rows_ms: int | None = None
    duration_count_ms: int | None = None


class ColumnSpecResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    key: str
    id: str
    value_type: int
    registry_id: int | None = None
    title: str
    description: str = ""
    width: int | None = None
    hidden: bool = False
    sortable: bool = False
    filterable: bool = False
    searchable: bool = False
    plugin_id: str | None = None


class AssetRow(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    asset_id: int = Field(alias="asset/id")
    asset_actor_id: int | None = Field(default=None, alias="asset/actor_id")
    asset_namespace: str | None = Field(default=None, alias="asset/namespace")
    asset_external_id: str | None = Field(default=None, alias="asset/external_id")
    asset_canonical_uri: str | None = Field(default=None, alias="asset/canonical_uri")


class AssetsListResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[AssetRow]
    schema_: list[ColumnSpecResponse] = Field(alias="schema")
    stats: QueryStats
    pagination: Pagination


class GroupedAssetsResponse(BaseModel):
    mode: str
    group_by: str
    group_value: str | None = None
    items: list[dict[str, Any]]
    stats: dict[str, Any]
    pagination: Pagination


class ChangesetChange(BaseModel):
    id: int
    asset_id: int
    actor_id: int
    changeset_id: int
    metadata_key: str
    metadata_key_id: int
    value_type: int
    value: Any
    removed: bool


class ChangesetChangesResponse(BaseModel):
    items: list[ChangesetChange]
    stats: QueryStats
    pagination: Pagination


class EditableMetadataSchemaResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_: dict[str, Any] = Field(alias="schema")
    uiSchema: dict[str, Any]
