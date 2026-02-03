from __future__ import annotations

from typing import Any, Iterable


def _normalize_params(params: Any) -> tuple[tuple[Any, ...], dict[str, Any]]:
    if params is None:
        return (), {}
    if isinstance(params, dict):
        return (), params
    if isinstance(params, (list, tuple)):
        return tuple(params), {}
    return (params,), {}


async def select(session: Any, sql: str, params: Any = None) -> list[dict[str, Any]]:
    args, kwargs = _normalize_params(params)
    return await session.select(sql, *args, **kwargs)


async def select_one(session: Any, sql: str, params: Any = None) -> dict[str, Any]:
    args, kwargs = _normalize_params(params)
    return await session.select_one(sql, *args, **kwargs)


async def select_one_or_none(
    session: Any, sql: str, params: Any = None
) -> dict[str, Any] | None:
    args, kwargs = _normalize_params(params)
    return await session.select_one_or_none(sql, *args, **kwargs)


async def execute(session: Any, sql: str, params: Any = None) -> Any:
    args, kwargs = _normalize_params(params)
    return await session.execute(sql, *args, **kwargs)


async def execute_many(
    session: Any, sql: str, params: Iterable[dict[str, Any]]
) -> Any:
    return await session.execute_many(sql, params)


async def scalar(session: Any, sql: str, params: Any = None) -> Any:
    row = await select_one(session, sql, params)
    if not row:
        return None
    return next(iter(row.values()))
