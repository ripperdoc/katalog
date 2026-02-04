from typing import Any, Mapping

from katalog.constants.metadata import METADATA_REGISTRY, MetadataKey, get_metadata_id
from katalog.db.sqlspec.tables import METADATA_TABLE
from katalog.models import MetadataType
from katalog.models.query import AssetFilter

from katalog.db.sqlspec.query_fields import asset_filter_fields


def _metadata_filter_condition(filt: Mapping[str, Any]) -> tuple[str, list[Any]]:
    """Build SQL predicate + params for a metadata-based filter."""

    accessor = filt.get("key")
    operator = filt.get("op")
    value = filt.get("value")
    values = filt.get("values")

    if accessor is None:
        raise ValueError("Filter accessor is required")

    key = MetadataKey(accessor)
    definition = METADATA_REGISTRY.get(key)
    if definition is None:
        raise ValueError(f"Filtering not supported for column: {accessor}")

    registry_id = get_metadata_id(definition.key)
    metadata_table = METADATA_TABLE

    col_map: dict[MetadataType, tuple[str, str]] = {
        MetadataType.STRING: ("m.value_text", "str"),
        MetadataType.INT: ("m.value_int", "int"),
        MetadataType.FLOAT: ("m.value_real", "float"),
        MetadataType.DATETIME: ("m.value_datetime", "datetime"),
        MetadataType.JSON: ("m.value_json", "str"),
        MetadataType.RELATION: ("m.value_relation_id", "int"),
        MetadataType.COLLECTION: ("m.value_collection_id", "int"),
    }
    try:
        column_name, col_type = col_map[definition.value_type]
    except KeyError:  # pragma: no cover
        raise ValueError(
            f"Unsupported metadata type for filtering: {definition.value_type}"
        )

    if definition.value_type == MetadataType.COLLECTION and operator in {
        "equals",
        "in",
        "notIn",
    }:
        if operator == "equals":
            if value is None:
                raise ValueError("Filter value is required")
            target_values = [int(value)]
        else:
            if not values:
                raise ValueError("Filter values are required")
            target_values = [int(v) for v in values]
        placeholders = ", ".join("?" for _ in target_values)
        latest_query = (
            "SELECT m.asset_id, m.value_collection_id, m.removed, "
            "ROW_NUMBER() OVER ("
            "    PARTITION BY m.asset_id, m.value_collection_id "
            "    ORDER BY m.changeset_id DESC, m.id DESC"
            ") AS rn "
            f"FROM {metadata_table} m "
            "WHERE m.metadata_key_id = ? "
            f"AND m.value_collection_id IN ({placeholders})"
        )
        exists_clause = (
            "EXISTS ("
            f"SELECT 1 FROM ({latest_query}) latest "
            "WHERE latest.asset_id = a.id AND latest.rn = 1 AND latest.removed = 0"
            ")"
        )
        if operator == "notIn":
            return f"NOT {exists_clause}", [registry_id, *target_values]
        return exists_clause, [registry_id, *target_values]

    def cast_value(val: Any) -> Any:
        if val is None:
            return None
        if col_type == "int":
            return int(val)
        if col_type == "float":
            return float(val)
        return val

    string_ops = {"contains", "notContains", "startsWith", "endsWith"}

    if operator in {
        "equals",
        "notEquals",
        "greaterThan",
        "lessThan",
        "greaterThanOrEqual",
        "lessThanOrEqual",
    }:
        if value is None:
            raise ValueError("Filter value is required")
        op_map = {
            "equals": "=",
            "notEquals": "!=",
            "greaterThan": ">",
            "lessThan": "<",
            "greaterThanOrEqual": ">=",
            "lessThanOrEqual": "<=",
        }
        predicate = f"{column_name} {op_map[operator]} ?"
        value_params = [cast_value(value)]
    elif col_type == "str" and operator in string_ops:
        if value is None:
            raise ValueError("Filter value is required")
        pattern = str(value)
        if operator == "contains":
            predicate = f"{column_name} LIKE ?"
            value_params = [f"%{pattern}%"]
        elif operator == "notContains":
            predicate = f"{column_name} NOT LIKE ?"
            value_params = [f"%{pattern}%"]
        elif operator == "startsWith":
            predicate = f"{column_name} LIKE ?"
            value_params = [f"{pattern}%"]
        else:  # endsWith
            predicate = f"{column_name} LIKE ?"
            value_params = [f"%{pattern}"]
    elif operator in {"between", "notBetween"}:
        if not values or len(values) != 2:
            raise ValueError("Filter values must contain two entries for between")
        op = "BETWEEN" if operator == "between" else "NOT BETWEEN"
        predicate = f"{column_name} {op} ? AND ?"
        value_params = [cast_value(values[0]), cast_value(values[1])]
    elif operator == "isEmpty":
        non_null = (
            f"{column_name} IS NOT NULL AND {column_name} != ''"
            if col_type == "str"
            else f"{column_name} IS NOT NULL"
        )
        condition = (
            "NOT EXISTS ("
            f"SELECT 1 FROM {metadata_table} m "
            "WHERE m.asset_id = a.id "
            "AND m.metadata_key_id = ? "
            "AND m.removed = 0 "
            "AND m.changeset_id = ("
            f"    SELECT MAX(m2.changeset_id) FROM {metadata_table} m2 "
            "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
            ") "
            f"AND {non_null}"
            ")"
        )
        return condition, [registry_id, registry_id]
    elif operator == "isNotEmpty":
        non_null = (
            f"{column_name} IS NOT NULL AND {column_name} != ''"
            if col_type == "str"
            else f"{column_name} IS NOT NULL"
        )
        condition = (
            "EXISTS ("
            f"SELECT 1 FROM {metadata_table} m "
            "WHERE m.asset_id = a.id "
            "AND m.metadata_key_id = ? "
            "AND m.removed = 0 "
            "AND m.changeset_id = ("
            f"    SELECT MAX(m2.changeset_id) FROM {metadata_table} m2 "
            "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
            ") "
            f"AND {non_null}"
            ")"
        )
        return condition, [registry_id, registry_id]
    else:
        raise ValueError(f"Unsupported filter operator: {operator}")

    condition = (
        "EXISTS ("
        f"SELECT 1 FROM {metadata_table} m "
        "WHERE m.asset_id = a.id "
        "AND m.metadata_key_id = ? "
        "AND m.removed = 0 "
        "AND m.changeset_id = ("
        f"    SELECT MAX(m2.changeset_id) FROM {metadata_table} m2 "
        "    WHERE m2.asset_id = a.id AND m2.metadata_key_id = ? AND m2.removed = 0"
        ") "
        f"AND {predicate}"
        ")"
    )
    params = [registry_id, registry_id, *value_params]
    return condition, params


def filter_conditions(filters):
    filters = filters or []
    conditions = []
    filter_params = []
    for raw in filters:
        if isinstance(raw, AssetFilter):
            filt = {
                "key": raw.key,
                "op": raw.op,
                "value": raw.value,
                "values": raw.values,
            }
        elif isinstance(raw, Mapping):
            filt = raw
        else:
            raise ValueError("Invalid filter format")
        accessor = filt.get("key")
        operator = filt.get("op")
        value = filt.get("value")
        values = filt.get("values")

        if accessor in asset_filter_fields:
            column_name, col_type = asset_filter_fields[accessor]

            def cast_value(val: Any) -> Any:
                if col_type == "int":
                    return int(val) if val is not None else None
                return val

            if operator in {
                "equals",
                "notEquals",
                "greaterThan",
                "lessThan",
                "greaterThanOrEqual",
                "lessThanOrEqual",
            }:
                if value is None:
                    raise ValueError("Filter value is required")
                op_map = {
                    "equals": "=",
                    "notEquals": "!=",
                    "greaterThan": ">",
                    "lessThan": "<",
                    "greaterThanOrEqual": ">=",
                    "lessThanOrEqual": "<=",
                }
                conditions.append(f"{column_name} {op_map[operator]} ?")
                filter_params.append(cast_value(value))
            elif col_type == "str" and operator in {
                "contains",
                "notContains",
                "startsWith",
                "endsWith",
            }:
                if value is None:
                    raise ValueError("Filter value is required")
                pattern = str(value)
                if operator == "contains":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"%{pattern}%")
                elif operator == "notContains":
                    conditions.append(f"{column_name} NOT LIKE ?")
                    filter_params.append(f"%{pattern}%")
                elif operator == "startsWith":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"{pattern}%")
                elif operator == "endsWith":
                    conditions.append(f"{column_name} LIKE ?")
                    filter_params.append(f"%{pattern}")
            elif operator in {"between", "notBetween"}:
                if not values or len(values) != 2:
                    raise ValueError(
                        "Filter values must contain two entries for between"
                    )
                op = "BETWEEN" if operator == "between" else "NOT BETWEEN"
                conditions.append(f"{column_name} {op} ? AND ?")
                filter_params.append(cast_value(values[0]))
                filter_params.append(cast_value(values[1]))
            elif operator == "isEmpty":
                conditions.append(f"{column_name} IS NULL")
            elif operator == "isNotEmpty":
                conditions.append(f"{column_name} IS NOT NULL")
            else:
                raise ValueError(f"Unsupported filter operator: {operator}")
        else:
            condition, params = _metadata_filter_condition(filt)
            conditions.append(condition)
            filter_params.extend(params)
    return conditions, filter_params
