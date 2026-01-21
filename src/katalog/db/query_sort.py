from katalog.constants.metadata import ASSET_ACTOR_ID, ASSET_ID
from katalog.views import ViewSpec

from katalog.db.query_fields import asset_sort_fields


def sort_conditions(sort: tuple[str, str] | None, view: ViewSpec):
    sort_col, sort_dir = (
        sort
        if sort is not None
        else (view.default_sort[0] if view.default_sort else (str(ASSET_ID), "asc"))
    )
    sort_dir = sort_dir.lower()
    if sort_dir not in {"asc", "desc"}:
        raise ValueError("sort direction must be 'asc' or 'desc'")
    sort_spec = view.column_map().get(sort_col)
    if sort_spec is None:
        raise ValueError(f"Unknown sort column: {sort_col}")
    if not sort_spec.sortable:
        raise ValueError(f"Sorting not supported for column: {sort_col}")

    if sort_col == str(ASSET_ACTOR_ID):
        raise ValueError("Sorting by actor is temporarily disabled")
    if sort_col not in asset_sort_fields:
        raise ValueError(f"Sorting not implemented for column: {sort_col}")
    return f"{asset_sort_fields[sort_col]} {sort_dir.upper()}, a.id ASC"
