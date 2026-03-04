from __future__ import annotations

from typing import Literal

from katalog.models.query import AssetQuery


def parse_sort_params(sort: list[str] | None) -> list[tuple[str, str]] | None:
    """Parse sort expressions into key/direction tuples."""
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
    metadata_include_linked_sidecars: bool | None = None,
    columns: list[str] | None = None,
    include_lost_assets: bool | None = None,
    search_mode: Literal["fts", "semantic", "hybrid"] | None = None,
    search_index: int | None = None,
    search_top_k: int | None = None,
    search_metadata_keys: list[str] | None = None,
    search_min_score: float | None = None,
    search_include_matches: bool | None = None,
    search_dimension: int | None = None,
    search_embedding_model: str | None = None,
    search_embedding_backend: Literal["preset", "fastembed"] | None = None,
) -> AssetQuery:
    """Build and validate an AssetQuery payload from request arguments."""
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
    if metadata_include_linked_sidecars is not None:
        payload["metadata_include_linked_sidecars"] = metadata_include_linked_sidecars
    if columns is not None:
        payload["columns"] = columns
    if include_lost_assets is not None:
        payload["include_lost_assets"] = include_lost_assets
    if search_mode is not None:
        payload["search_mode"] = search_mode
    if search_index is not None:
        payload["search_index"] = search_index
    if search_top_k is not None:
        payload["search_top_k"] = search_top_k
    if search_metadata_keys is not None:
        payload["search_metadata_keys"] = search_metadata_keys
    if search_min_score is not None:
        payload["search_min_score"] = search_min_score
    if search_include_matches is not None:
        payload["search_include_matches"] = search_include_matches
    if search_dimension is not None:
        payload["search_dimension"] = search_dimension
    if search_embedding_model is not None:
        payload["search_embedding_model"] = search_embedding_model
    if search_embedding_backend is not None:
        payload["search_embedding_backend"] = search_embedding_backend
    return AssetQuery.model_validate(payload)
