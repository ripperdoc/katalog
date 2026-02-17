from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from katalog.constants.metadata import MetadataType
from katalog.models.views import get_view


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


class AssetFilter(BaseModel):
    key: str
    op: str
    value: str | None = None
    values: list[str] | None = None


class AssetQuery(BaseModel):
    """Query options for listing assets with metadata projections."""

    view_id: str | None = None

    # Asset filters/sorts/search.
    filters: list[AssetFilter] | None = None
    search: str | None = None
    search_mode: Literal["fts", "semantic", "hybrid"] = "fts"
    search_index: int | None = None
    search_granularity: Literal["asset", "metadata"] = "asset"
    search_top_k: int | None = Field(default=None, gt=0)
    search_metadata_keys: list[str] | None = None
    search_min_score: float | None = None
    search_include_matches: bool = False
    search_dimension: int = Field(default=64, gt=0)
    search_embedding_model: str = "fast"
    search_embedding_backend: Literal["preset", "fastembed"] = "preset"
    sort: list[tuple[str, str]] | None = None
    group_by: str | None = None

    # Pagination.
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=100, gt=0)

    # Metadata projection controls.
    metadata_actor_ids: list[int] | None = None
    metadata_include_removed: bool = False
    metadata_aggregation: Literal["latest", "array", "objects"] = "latest"
    metadata_include_counts: bool = True

    @field_validator("view_id")
    @classmethod
    def _validate_view_id(cls, value: str | None) -> str | None:
        if value is None:
            return value
        get_view(value)
        return value

    @field_validator("metadata_actor_ids")
    @classmethod
    def _validate_metadata_actor_ids(cls, value: list[int] | None) -> list[int] | None:
        if value is None:
            return value
        if any(actor_id <= 0 for actor_id in value):
            raise ValueError("metadata_actor_ids must contain positive integers")
        return value

    @field_validator("search_metadata_keys")
    @classmethod
    def _validate_search_metadata_keys(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        cleaned = [item.strip() for item in value if item and item.strip()]
        return cleaned or None

    @field_validator("filters", mode="before")
    @classmethod
    def _parse_filters(cls, value: Any) -> list[AssetFilter] | None:
        if value is None:
            return None
        if isinstance(value, list):
            parsed: list[AssetFilter] = []
            for item in value:
                if isinstance(item, AssetFilter):
                    parsed.append(item)
                    continue
                if isinstance(item, str):
                    key, operator, raw_value = _parse_filter(item)
                    if operator in {"between", "notBetween", "in", "notIn"}:
                        parsed.append(
                            AssetFilter(
                                key=key,
                                op=operator,
                                values=_split_values(raw_value),
                            )
                        )
                    elif operator in {"isEmpty", "isNotEmpty"}:
                        parsed.append(AssetFilter(key=key, op=operator))
                    else:
                        parsed.append(AssetFilter(key=key, op=operator, value=raw_value))
                else:
                    parsed.append(AssetFilter.model_validate(item))
            return parsed
        return [AssetFilter.model_validate(value)]


    @model_validator(mode="after")
    def _validate_query(self) -> "AssetQuery":
        if self.view_id is None:
            self.view_id = "default"
        view = get_view(self.view_id)
        column_map = view.column_map()

        if self.filters:
            for filt in self.filters:
                key = filt.key
                operator = filt.op
                value = filt.value or ""
                column = column_map.get(key)
                if column is None:
                    raise ValueError(f"Unknown filter key: {key}")
                allowed = _allowed_operators(column.value_type)
                if operator not in allowed:
                    raise ValueError(
                        f"Operator {operator} not valid for {key} ({column.value_type})"
                    )
                if operator in {"between", "notBetween", "in", "notIn"}:
                    if not filt.values:
                        raise ValueError(
                            f"Operator {operator} requires comma-separated values"
                        )
                if operator in {"isEmpty", "isNotEmpty"} and value.strip() == "":
                    raise ValueError(
                        f"Operator {operator} requires a value placeholder"
                    )

        if self.sort:
            for item in self.sort:
                if len(item) != 2:
                    raise ValueError("sort entries must be (key, direction)")
                key, direction = item
                if key not in column_map:
                    raise ValueError(f"Unknown sort key: {key}")
                if direction not in {"asc", "desc"}:
                    raise ValueError("sort direction must be asc or desc")

        if self.group_by is not None and self.group_by not in column_map:
            raise ValueError(f"Unknown group_by key: {self.group_by}")

        if self.search_mode in {"semantic", "hybrid"}:
            if not self.search or not self.search.strip():
                raise ValueError("search is required for semantic search modes")
            if self.search_granularity == "metadata" and self.group_by is not None:
                raise ValueError("group_by is not supported for metadata granularity")

        return self


def _parse_filter(raw: str) -> tuple[str, str, str]:
    parts = raw.split(" ", 2)
    if len(parts) != 3:
        raise ValueError("filter must have form: <key> <operator> <value>")
    key, operator, value = (part.strip() for part in parts)
    if not key:
        raise ValueError("filter key is required")
    if not operator:
        raise ValueError("filter operator is required")
    if value == "":
        raise ValueError("filter value is required")
    return key, operator, value


def _split_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _allowed_operators(value_type: MetadataType) -> set[str]:
    string_ops = {
        "equals",
        "notEquals",
        "contains",
        "notContains",
        "startsWith",
        "endsWith",
        "isEmpty",
        "isNotEmpty",
    }
    number_ops = {
        "equals",
        "notEquals",
        "greaterThan",
        "lessThan",
        "greaterThanOrEqual",
        "lessThanOrEqual",
        "between",
        "notBetween",
        "isEmpty",
        "isNotEmpty",
    }
    bool_ops = {"equals", "isEmpty", "isNotEmpty"}
    date_ops = {
        "equals",
        "notEquals",
        "before",
        "after",
        "between",
        "notBetween",
        "isEmpty",
        "isNotEmpty",
    }
    enum_ops = {"in", "notIn", "isEmpty", "isNotEmpty"}
    json_ops = {"equals", "notEquals", "isEmpty", "isNotEmpty"}

    if value_type == MetadataType.STRING:
        return string_ops
    if value_type in {MetadataType.INT, MetadataType.FLOAT}:
        return number_ops
    if value_type == MetadataType.DATETIME:
        return date_ops
    if value_type in {MetadataType.RELATION, MetadataType.COLLECTION}:
        return enum_ops
    if value_type == MetadataType.JSON:
        return json_ops
    return bool_ops
