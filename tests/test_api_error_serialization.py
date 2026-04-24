from __future__ import annotations

import json

import pytest
from starlette.requests import Request

from katalog.api.helpers import ApiError, validate_and_normalize_config
from katalog.server.app import api_error_handler
from katalog.sources.tabular import TabularSourceConfig


class _DummyTabularPlugin:
    config_model = TabularSourceConfig


def test_validate_and_normalize_config_returns_json_safe_structured_errors() -> None:
    config = {
        "namespace": "products",
        "column_mappings": [
            {"column": "Description", "key": "file/description", "value_type": "string"},
            {
                "column": "Description",
                "key": "file/description_duplicate",
                "value_type": "string",
            },
        ],
    }

    with pytest.raises(ApiError) as exc_info:
        validate_and_normalize_config(_DummyTabularPlugin, config)

    err = exc_info.value
    assert err.status_code == 400
    assert isinstance(err.detail, dict)
    assert err.detail.get("message") == "Invalid config"
    errors = err.detail.get("errors")
    assert isinstance(errors, list)
    assert errors
    first = errors[0]
    assert first.get("type") == "value_error"
    assert "Duplicate column mapping" in str(first.get("msg"))
    assert first.get("loc") in ((), [])
    assert "ctx" not in first
    assert "input" not in first


@pytest.mark.asyncio
async def test_api_error_handler_stringifies_nested_exceptions_in_detail() -> None:
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": []})
    response = await api_error_handler(
        request,
        ApiError(status_code=400, detail={"error": ValueError("boom")}),
    )

    assert response.status_code == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["detail"]["error"] == "boom"
