from __future__ import annotations

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

