from __future__ import annotations

import json

from katalog.models.query import AssetQuery


def parse_sort_params(sort: list[str] | None) -> list[tuple[str, str]] | None:
    if not sort:
        return None
    result: list[tuple[str, str]] = []
    for raw in sort:
        if ":" in raw:
            key, direction = raw.split(":", 1)
        else:
            key, direction = raw, "asc"
        key = key.strip()
        direction = direction.strip() or "asc"
        if key:
            result.append((key, direction))
    return result or None


def filters_to_db_filters(filters: list[str] | None) -> list[str] | None:
    if not filters:
        return None
    converted: list[str] = []
    for raw in filters:
        key, operator, value = _parse_filter(raw)
        payload: dict[str, object] = {"accessor": key, "operator": operator}
        if operator in {"between", "notBetween", "in", "notIn"}:
            payload["values"] = _split_values(value)
        elif operator in {"isEmpty", "isNotEmpty"}:
            payload["value"] = None
        else:
            payload["value"] = value
        converted.append(json.dumps(payload))
    return converted or None


def build_asset_query(
    *,
    view_id: str | None,
    offset: int,
    limit: int,
    sort: list[str] | None,
    filters: list[str] | None,
    search: str | None,
    group_by: str | None = None,
    metadata_actor_ids: list[int] | None = None,
    metadata_include_removed: bool | None = None,
    metadata_aggregation: str | None = None,
    metadata_include_counts: bool | None = None,
) -> AssetQuery:
    payload: dict[str, object] = {
        "view_id": view_id,
        "offset": offset,
        "limit": limit,
        "sort": parse_sort_params(sort),
        "filters": filters,
        "search": search,
        "group_by": group_by,
    }
    if metadata_actor_ids is not None:
        payload["metadata_actor_ids"] = metadata_actor_ids
    if metadata_include_removed is not None:
        payload["metadata_include_removed"] = metadata_include_removed
    if metadata_aggregation is not None:
        payload["metadata_aggregation"] = metadata_aggregation
    if metadata_include_counts is not None:
        payload["metadata_include_counts"] = metadata_include_counts
    return AssetQuery.model_validate(payload)


def _parse_filter(raw: str) -> tuple[str, str, str]:
    parts = raw.split(" ", 2)
    if len(parts) != 3:
        raise ValueError("filter must have form: <key> <operator> <value>")
    key, operator, value = (part.strip() for part in parts)
    return key, operator, value


def _split_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]
