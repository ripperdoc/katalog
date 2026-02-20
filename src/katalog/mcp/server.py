from __future__ import annotations

from typing import Any, Literal

from fastmcp import FastMCP

from katalog.api import actors, assets, collections, metadata, views
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query


def _tool_error(exc: ApiError) -> ValueError:
    return ValueError(f"API error {exc.status_code}: {exc.detail}")


def _jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


def _validate_pagination(offset: int, limit: int, *, max_limit: int = 1000) -> None:
    if offset < 0:
        raise ValueError("offset must be >= 0")
    if limit < 1 or limit > max_limit:
        raise ValueError(f"limit must be between 1 and {max_limit}")


def create_mcp_server() -> FastMCP:
    mcp = FastMCP(
        name="katalog",
        instructions=(
            "Read-only access to katalog workspace data. "
            "Use these tools for listing and inspecting assets, views, collections, actors, and metadata."
        ),
    )

    @mcp.tool(
        name="views.list",
        description="List available asset views.",
    )
    async def list_views() -> dict[str, Any]:
        return {"views": _jsonable(views.list_views())}

    @mcp.tool(
        name="views.get",
        description="Get view configuration by view id.",
    )
    async def get_view(view_id: str) -> dict[str, Any]:
        try:
            view = await views.get_view_api(view_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"view": _jsonable(view)}

    @mcp.tool(
        name="views.list_assets",
        description=(
            "List assets for a view with pagination, filtering, sorting and metadata projection options."
        ),
    )
    async def list_view_assets(
        view_id: str = "default",
        offset: int = 0,
        limit: int = 100,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
        metadata_actor_ids: list[int] | None = None,
        metadata_include_removed: bool = False,
        metadata_aggregation: Literal["latest", "array", "objects"] | None = None,
        metadata_include_counts: bool = True,
    ) -> dict[str, Any]:
        """Use `sort` as `key:asc|desc` and `filters` as `<key> <op> <value>` strings."""
        _validate_pagination(offset=offset, limit=limit)
        try:
            query = build_asset_query(
                view_id=view_id,
                offset=offset,
                limit=limit,
                sort=sort,
                filters=filters,
                search=search,
                metadata_actor_ids=metadata_actor_ids,
                metadata_include_removed=metadata_include_removed,
                metadata_aggregation=metadata_aggregation,
                metadata_include_counts=metadata_include_counts,
            )
            response = await views.list_assets_for_view(view_id=view_id, query=query)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return _jsonable(response)

    @mcp.tool(
        name="assets.list",
        description="List assets from the default view.",
    )
    async def list_assets(
        offset: int = 0,
        limit: int = 100,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
        metadata_actor_ids: list[int] | None = None,
        metadata_include_removed: bool = False,
        metadata_aggregation: Literal["latest", "array", "objects"] | None = None,
        metadata_include_counts: bool = True,
    ) -> dict[str, Any]:
        return await list_view_assets(
            view_id="default",
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            search=search,
            metadata_actor_ids=metadata_actor_ids,
            metadata_include_removed=metadata_include_removed,
            metadata_aggregation=metadata_aggregation,
            metadata_include_counts=metadata_include_counts,
        )

    @mcp.tool(
        name="assets.grouped",
        description="List grouped assets using a group_by column key.",
    )
    async def list_grouped_assets(
        group_by: str,
        offset: int = 0,
        limit: int = 50,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
        metadata_actor_ids: list[int] | None = None,
        metadata_include_removed: bool = False,
        metadata_aggregation: Literal["latest", "array", "objects"] | None = None,
        metadata_include_counts: bool = True,
    ) -> dict[str, Any]:
        """Use `sort` as `key:asc|desc` and `filters` as `<key> <op> <value>` strings."""
        _validate_pagination(offset=offset, limit=limit, max_limit=500)
        try:
            query = build_asset_query(
                view_id="default",
                offset=offset,
                limit=limit,
                sort=sort,
                filters=filters,
                search=search,
                group_by=group_by,
                metadata_actor_ids=metadata_actor_ids,
                metadata_include_removed=metadata_include_removed,
                metadata_aggregation=metadata_aggregation,
                metadata_include_counts=metadata_include_counts,
            )
            response = await assets.list_grouped_assets(group_by=group_by, query=query)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return _jsonable(response)

    @mcp.tool(
        name="assets.get",
        description="Get one asset with all metadata entries, including removed metadata rows.",
    )
    async def get_asset(asset_id: int) -> dict[str, Any]:
        try:
            asset, metadata_items = await assets.get_asset(asset_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"asset": _jsonable(asset), "metadata": _jsonable(metadata_items)}

    @mcp.tool(
        name="collections.list",
        description="List all collections.",
    )
    async def list_collections() -> dict[str, Any]:
        collection_list = await collections.list_collections()
        return {"collections": _jsonable(collection_list)}

    @mcp.tool(
        name="collections.get",
        description="Get collection details by id.",
    )
    async def get_collection(collection_id: int) -> dict[str, Any]:
        try:
            collection = await collections.get_collection(collection_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"collection": _jsonable(collection)}

    @mcp.tool(
        name="collections.list_assets",
        description="List assets that belong to a collection.",
    )
    async def list_collection_assets(
        collection_id: int,
        view_id: str = "default",
        offset: int = 0,
        limit: int = 100,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
        metadata_actor_ids: list[int] | None = None,
        metadata_include_removed: bool = False,
        metadata_aggregation: Literal["latest", "array", "objects"] | None = None,
        metadata_include_counts: bool = True,
    ) -> dict[str, Any]:
        """Use `sort` as `key:asc|desc` and `filters` as `<key> <op> <value>` strings."""
        _validate_pagination(offset=offset, limit=limit)
        try:
            query = build_asset_query(
                view_id=view_id,
                offset=offset,
                limit=limit,
                sort=sort,
                filters=filters,
                search=search,
                metadata_actor_ids=metadata_actor_ids,
                metadata_include_removed=metadata_include_removed,
                metadata_aggregation=metadata_aggregation,
                metadata_include_counts=metadata_include_counts,
            )
            response = await collections.list_collection_assets(
                collection_id=collection_id,
                query=query,
            )
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return _jsonable(response)

    @mcp.tool(
        name="actors.list",
        description="List all actors.",
    )
    async def list_actors() -> dict[str, Any]:
        actor_list = await actors.list_actors()
        return {"actors": _jsonable(actor_list)}

    @mcp.tool(
        name="actors.get",
        description="Get actor details by id, including related changesets.",
    )
    async def get_actor(actor_id: int) -> dict[str, Any]:
        try:
            actor, actor_changesets = await actors.get_actor(actor_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {
            "actor": _jsonable(actor),
            "changesets": _jsonable(actor_changesets),
        }

    @mcp.tool(
        name="metadata.schema_editable",
        description="Get editable metadata JSON schema and UI schema.",
    )
    async def metadata_schema_editable() -> dict[str, Any]:
        payload = await metadata.metadata_schema_editable()
        return _jsonable(payload)

    @mcp.tool(
        name="metadata.registry",
        description="Get metadata registry indexed by numeric key id.",
    )
    async def metadata_registry() -> dict[str, Any]:
        payload = await metadata.metadata_registry()
        return _jsonable(payload)

    return mcp


def create_mcp_http_app(path: str = "/") -> Any:
    mcp = create_mcp_server()
    return mcp.http_app(path=path, transport="streamable-http")
