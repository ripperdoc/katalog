from __future__ import annotations

from typing import Any
from datetime import datetime, timezone
from enum import Enum


def build_where(filters: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if not filters:
        return "", {}
    clauses = []
    params: dict[str, Any] = {}
    for key, value in filters.items():
        if value is None:
            continue
        if key.endswith("__in"):
            column = key[: -len("__in")]
            values = list(value)
            if not values:
                clauses.append("1 = 0")
                continue
            placeholders = []
            for idx, item in enumerate(values):
                if isinstance(item, Enum):
                    item = item.value
                p_key = f"f_{column}_{idx}"
                placeholders.append(f":{p_key}")
                params[p_key] = item
            clauses.append(f"{column} IN ({', '.join(placeholders)})")
            continue
        if isinstance(value, Enum):
            value = value.value
        param_key = f"f_{key}"
        clauses.append(f"{key} = :{param_key}")
        params[param_key] = value
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def to_utc_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    raise TypeError(f"Unsupported datetime value: {value!r}")


def datetime_to_iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise TypeError(f"Unsupported datetime value: {value!r}")
