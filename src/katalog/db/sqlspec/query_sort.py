from katalog.constants.metadata import ASSET_ACTOR_ID, ASSET_ID, MetadataKey, MetadataType, get_metadata_id
from katalog.models.views import ViewSpec

from katalog.db.sqlspec.query_fields import asset_sort_fields


def _metadata_sort_expr(sort_col: str, view: ViewSpec) -> str:
    spec = view.column_map().get(sort_col)
    if spec is None or spec.registry_id is None:
        raise ValueError(f"Unknown sort column: {sort_col}")
    if spec.value_type == MetadataType.INT:
        value_col = "value_int"
    elif spec.value_type == MetadataType.FLOAT:
        value_col = "value_real"
    elif spec.value_type == MetadataType.DATETIME:
        value_col = "value_datetime"
    elif spec.value_type == MetadataType.STRING:
        value_col = "value_text"
    else:
        raise ValueError(f"Sorting not supported for metadata type: {spec.value_type}")

    metadata_key_id = get_metadata_id(MetadataKey(sort_col))
    return (
        "("
        "SELECT m.{value_col} "
        "FROM metadata m "
        "WHERE m.asset_id = a.id "
        "AND m.metadata_key_id = {metadata_key_id} "
        "AND m.removed = 0 "
        "ORDER BY m.changeset_id DESC, m.id DESC "
        "LIMIT 1"
        ")"
    ).format(value_col=value_col, metadata_key_id=int(metadata_key_id))


def sort_conditions(
    sort: tuple[str, str] | None,
    view: ViewSpec,
    *,
    metadata_aggregation: str = "latest",
):
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
    if sort_col in asset_sort_fields:
        return f"{asset_sort_fields[sort_col]} {sort_dir.upper()}, a.id ASC"

    if metadata_aggregation != "latest":
        raise ValueError("Sorting only supports metadata_aggregation=latest for now")

    metadata_expr = _metadata_sort_expr(sort_col, view)
    return f"{metadata_expr} {sort_dir.upper()}, a.id ASC"
