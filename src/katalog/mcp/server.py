from __future__ import annotations

from typing import Any, Literal

from fastmcp import FastMCP

from katalog.api import (
    actors,
    assets,
    changesets,
    collections,
    metadata,
    plugins,
    system,
    views,
    workflows,
)
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query
from katalog.help_texts import (
    MCP_ACTORS_GET_DESC,
    MCP_ACTORS_GET_CONFIG_SCHEMA_DESC,
    MCP_ACTORS_LIST_DESC,
    MCP_ASSETS_GET_DESC,
    MCP_ASSETS_GROUPED_DESC,
    MCP_ASSETS_LIST_DESC,
    MCP_CHANGESETS_CHANGES_DESC,
    MCP_CHANGESETS_GET_DESC,
    MCP_CHANGESETS_LIST_DESC,
    MCP_COLLECTIONS_GET_DESC,
    MCP_COLLECTIONS_LIST_ASSETS_DESC,
    MCP_COLLECTIONS_LIST_DESC,
    MCP_INSTRUCTIONS,
    MCP_METADATA_SEARCH_DESC,
    MCP_METADATA_REGISTRY_DESC,
    MCP_METADATA_SCHEMA_EDITABLE_DESC,
    MCP_PLUGINS_GET_CONFIG_SCHEMA_DESC,
    MCP_PLUGINS_LIST_DESC,
    MCP_SYSTEM_STATS_DESC,
    MCP_VIEWS_GET_DESC,
    MCP_VIEWS_LIST_DESC,
    MCP_WORKFLOWS_GET_DESC,
    MCP_WORKFLOWS_LIST_DESC,
)


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
        instructions=MCP_INSTRUCTIONS,
    )

    @mcp.tool(
        name="views.list",
        description=MCP_VIEWS_LIST_DESC,
    )
    async def list_views() -> dict[str, Any]:
        view_list = await views.list_views_api()
        return {"views": _jsonable(view_list)}

    @mcp.tool(
        name="views.get",
        description=MCP_VIEWS_GET_DESC,
    )
    async def get_view(view_id: str) -> dict[str, Any]:
        try:
            view = await views.get_view_api(view_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"view": _jsonable(view)}

    @mcp.tool(
        name="assets.list",
        description=MCP_ASSETS_LIST_DESC,
    )
    async def list_assets(
        view_id: str = "default",
        offset: int = 0,
        limit: int = 100,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
        search_mode: Literal["fts", "semantic", "hybrid"] | None = None,
        search_index: int | None = None,
        search_top_k: int | None = None,
        search_metadata_keys: list[str] | None = None,
        search_min_score: float | None = None,
        search_include_matches: bool = False,
        search_dimension: int | None = None,
        search_embedding_model: str | None = None,
        search_embedding_backend: Literal["preset", "fastembed"] | None = None,
        metadata_actor_ids: list[int] | None = None,
        metadata_include_removed: bool = False,
        metadata_aggregation: Literal["latest", "array", "objects"] | None = None,
        metadata_include_counts: bool = True,
        metadata_include_linked_sidecars: bool = False,
        columns: list[str] | None = None,
        include_lost_assets: bool = False,
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
                search_mode=search_mode,
                search_index=search_index,
                search_top_k=search_top_k,
                search_metadata_keys=search_metadata_keys,
                search_min_score=search_min_score,
                search_include_matches=search_include_matches,
                search_dimension=search_dimension,
                search_embedding_model=search_embedding_model,
                search_embedding_backend=search_embedding_backend,
                metadata_actor_ids=metadata_actor_ids,
                metadata_include_removed=metadata_include_removed,
                metadata_aggregation=metadata_aggregation,
                metadata_include_counts=metadata_include_counts,
                metadata_include_linked_sidecars=metadata_include_linked_sidecars,
                columns=columns,
                include_lost_assets=include_lost_assets,
            )
            response = await assets.list_assets(query=query)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return _jsonable(response)

    @mcp.tool(
        name="assets.grouped",
        description=MCP_ASSETS_GROUPED_DESC,
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
        include_lost_assets: bool = False,
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
                include_lost_assets=include_lost_assets,
            )
            response = await assets.list_grouped_assets(group_by=group_by, query=query)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        return _jsonable(response)

    @mcp.tool(
        name="assets.get",
        description=MCP_ASSETS_GET_DESC,
    )
    async def get_asset(asset_id: int) -> dict[str, Any]:
        try:
            asset, metadata_items = await assets.get_asset(asset_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"asset": _jsonable(asset), "metadata": _jsonable(metadata_items)}

    @mcp.tool(
        name="collections.list",
        description=MCP_COLLECTIONS_LIST_DESC,
    )
    async def list_collections() -> dict[str, Any]:
        collection_list = await collections.list_collections()
        return {"collections": _jsonable(collection_list)}

    @mcp.tool(
        name="collections.get",
        description=MCP_COLLECTIONS_GET_DESC,
    )
    async def get_collection(collection_id: int) -> dict[str, Any]:
        try:
            collection = await collections.get_collection(collection_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"collection": _jsonable(collection)}

    @mcp.tool(
        name="collections.list_assets",
        description=MCP_COLLECTIONS_LIST_ASSETS_DESC,
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
        description=MCP_ACTORS_LIST_DESC,
    )
    async def list_actors() -> dict[str, Any]:
        actor_list = await actors.list_actors()
        return {"actors": _jsonable(actor_list)}

    @mcp.tool(
        name="actors.get",
        description=MCP_ACTORS_GET_DESC,
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
        name="actors.get_config_schema",
        description=MCP_ACTORS_GET_CONFIG_SCHEMA_DESC,
    )
    async def get_actor_config_schema(actor_id: int) -> dict[str, Any]:
        try:
            payload = await actors.get_actor_config_schema(actor_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return _jsonable(payload)

    @mcp.tool(
        name="changesets.list",
        description=MCP_CHANGESETS_LIST_DESC,
    )
    async def list_changesets() -> dict[str, Any]:
        changeset_list = await changesets.list_changesets()
        return {"changesets": _jsonable(changeset_list)}

    @mcp.tool(
        name="changesets.get",
        description=MCP_CHANGESETS_GET_DESC,
    )
    async def get_changeset(changeset_id: int) -> dict[str, Any]:
        try:
            changeset_item, logs, running = await changesets.get_changeset(changeset_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {
            "changeset": _jsonable(changeset_item),
            "logs": _jsonable(logs),
            "running": running,
        }

    @mcp.tool(
        name="changesets.changes",
        description=MCP_CHANGESETS_CHANGES_DESC,
    )
    async def list_changeset_changes(
        changeset_id: int,
        view: Literal["raw", "diff"] = "raw",
        offset: int = 0,
        limit: int = 200,
        from_changeset_id: int | None = None,
        to_changeset_id: int | None = None,
        sort: list[str] | None = None,
        filters: list[str] | None = None,
        search: str | None = None,
    ) -> dict[str, Any]:
        _validate_pagination(offset=offset, limit=limit, max_limit=1000)
        try:
            if view == "diff":
                response = await changesets.list_changeset_diff(
                    changeset_id=changeset_id,
                    offset=offset,
                    limit=limit,
                    from_changeset_id=from_changeset_id,
                    to_changeset_id=to_changeset_id,
                    sort=sort,
                    filters=filters,
                    search=search,
                )
                return _jsonable(response)
            response = await changesets.list_changeset_changes(
                changeset_id=changeset_id,
                offset=offset,
                limit=limit,
                from_changeset_id=from_changeset_id,
                to_changeset_id=to_changeset_id,
            )
            return _jsonable(response)
        except ApiError as exc:
            raise _tool_error(exc) from exc

    @mcp.tool(
        name="metadata.schema_editable",
        description=MCP_METADATA_SCHEMA_EDITABLE_DESC,
    )
    async def metadata_schema_editable() -> dict[str, Any]:
        payload = await metadata.metadata_schema_editable()
        return _jsonable(payload)

    @mcp.tool(
        name="metadata.registry",
        description=MCP_METADATA_REGISTRY_DESC,
    )
    async def metadata_registry() -> dict[str, Any]:
        payload = await metadata.metadata_registry()
        return _jsonable(payload)

    @mcp.tool(
        name="metadata.search",
        description=MCP_METADATA_SEARCH_DESC,
    )
    async def list_metadata(
        query: str | None = None,
        search_mode: Literal["fts", "semantic", "hybrid"] = "fts",
        limit: int = 50,
        offset: int = 0,
        filters: list[str] | None = None,
        metadata_keys: list[str] | None = None,
        actor_ids: list[int] | None = None,
        include_removed: bool = False,
        aggregation: Literal["latest", "array", "objects"] = "latest",
        search_index: int | None = None,
        top_k: int = 100,
        min_score: float | None = None,
        dimension: int = 64,
        embedding_model: str = "fast",
        embedding_backend: Literal["preset", "fastembed"] = "preset",
    ) -> dict[str, Any]:
        _validate_pagination(offset=offset, limit=limit, max_limit=10000)
        try:
            asset_query = build_asset_query(
                view_id="default",
                offset=offset,
                limit=limit,
                sort=None,
                filters=filters,
                search=query,
                search_mode=search_mode,
                search_index=search_index,
                search_top_k=top_k,
                search_metadata_keys=metadata_keys,
                search_min_score=min_score,
                search_dimension=dimension,
                search_embedding_model=embedding_model,
                search_embedding_backend=embedding_backend,
                metadata_actor_ids=actor_ids,
                metadata_include_removed=include_removed,
                metadata_aggregation=aggregation,
            ).model_copy(update={"search_granularity": "metadata"})
            payload = await metadata.list_metadata(asset_query)
            return _jsonable(payload)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

    @mcp.tool(
        name="system.stats",
        description=MCP_SYSTEM_STATS_DESC,
    )
    async def workspace_size_stats() -> dict[str, Any]:
        payload = await system.workspace_size_stats()
        return _jsonable(payload)

    @mcp.tool(
        name="plugins.list",
        description=MCP_PLUGINS_LIST_DESC,
    )
    async def list_plugins() -> dict[str, Any]:
        plugin_list = await plugins.list_plugins()
        return {"plugins": _jsonable(plugin_list)}

    @mcp.tool(
        name="plugins.get_config_schema",
        description=MCP_PLUGINS_GET_CONFIG_SCHEMA_DESC,
    )
    async def get_plugin_config_schema(plugin_id: str) -> dict[str, Any]:
        try:
            payload = await plugins.get_plugin_config_schema(plugin_id)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return _jsonable(payload)

    @mcp.tool(
        name="workflows.list",
        description=MCP_WORKFLOWS_LIST_DESC,
    )
    async def list_workflows() -> dict[str, Any]:
        workflow_list = await workflows.list_workflows()
        return {"workflows": _jsonable(workflow_list)}

    @mcp.tool(
        name="workflows.get",
        description=MCP_WORKFLOWS_GET_DESC,
    )
    async def get_workflow(workflow_name: str) -> dict[str, Any]:
        try:
            payload = await workflows.get_workflow(workflow_name)
        except ApiError as exc:
            raise _tool_error(exc) from exc
        return {"workflow": _jsonable(payload)}

    return mcp


def create_mcp_http_app(path: str = "/") -> Any:
    mcp = create_mcp_server()
    return mcp.http_app(path=path, transport="streamable-http")
