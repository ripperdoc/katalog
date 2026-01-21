from typing import Any, Mapping

from katalog.models import MetadataType


def _decode_metadata_value(row: Mapping[str, Any]) -> Any:
    """Decode a metadata row's value based on its value_type.

    Shared between asset listings and changeset change listings to keep value
    handling consistent.
    """

    value_type_raw = row.get("value_type")
    if value_type_raw is None:
        return None
    value_type = (
        value_type_raw
        if isinstance(value_type_raw, MetadataType)
        else MetadataType(int(value_type_raw))
    )

    if value_type == MetadataType.STRING:
        return row.get("value_text")
    if value_type == MetadataType.INT:
        return row.get("value_int")
    if value_type == MetadataType.FLOAT:
        return row.get("value_real")
    if value_type == MetadataType.DATETIME:
        dt = row.get("value_datetime")
        return dt.isoformat() if dt and hasattr(dt, "isoformat") else dt
    if value_type == MetadataType.JSON:
        return row.get("value_json")
    if value_type == MetadataType.RELATION:
        return row.get("value_relation_id")
    return None
