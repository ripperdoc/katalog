from typing import Any, Optional

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from katalog.constants.metadata import COLLECTION_MEMBER, get_metadata_id
from tortoise import Tortoise

from katalog.db import list_assets_for_view
from katalog.db.query_assets import build_assets_where, count_assets_for_query
from katalog.constants.metadata import MetadataType
from katalog.models import (
    Asset,
    AssetCollection,
    Changeset,
    Metadata,
    CollectionRefreshMode,
    OpStatus,
    make_metadata,
)
from katalog.models.views import get_view
from katalog.editors.user_editor import ensure_user_editor
from katalog.api.helpers import ApiError

router = APIRouter()


class CollectionCreate(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None
    asset_ids: list[int] = Field(default_factory=list)
    source: dict[str, Any] | None = None
    refresh_mode: str | CollectionRefreshMode | None = None


class CollectionUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    refresh_mode: str | CollectionRefreshMode | None = None


async def list_collections_api() -> dict[str, Any]:
    collections = await AssetCollection.all().order_by("-created_at")
    result = []
    for col in collections:
        result.append(col.to_dict())
    return {"collections": result}


async def create_collection_api(payload: CollectionCreate) -> dict[str, Any]:
    try:
        asset_ids = [int(a) for a in payload.asset_ids]
    except Exception:
        raise ApiError(status_code=400, detail="asset_ids must be integers")

    if payload.source and not isinstance(payload.source, dict):
        raise ApiError(status_code=400, detail="source must be an object")

    query_payload = None
    if payload.source:
        query_payload = payload.source.get("query")
        if query_payload is not None and not isinstance(query_payload, dict):
            raise ApiError(status_code=400, detail="source.query must be an object")

    if query_payload and asset_ids:
        raise ApiError(
            status_code=400, detail="Provide either asset_ids or source.query, not both"
        )

    existing = await AssetCollection.get_or_none(name=payload.name)
    if existing:
        raise ApiError(status_code=400, detail="Collection name already exists")

    refresh_mode = payload.refresh_mode or CollectionRefreshMode.ON_DEMAND
    if isinstance(refresh_mode, str):
        try:
            refresh_mode = CollectionRefreshMode(refresh_mode)
        except Exception:
            raise ApiError(
                status_code=400,
                detail="refresh_mode must be 'live' or 'on_demand'",
            )

    if refresh_mode == CollectionRefreshMode.LIVE and not query_payload:
        raise ApiError(
            status_code=400,
            detail="refresh_mode 'live' requires source.query",
        )

    if query_payload:
        view_id = str(query_payload.get("view_id") or "default")
        try:
            get_view(view_id)
        except KeyError:
            raise ApiError(status_code=404, detail="View not found")

        sort = query_payload.get("sort")
        if sort is not None and not isinstance(sort, str):
            raise ApiError(status_code=400, detail="source.query.sort must be a string")

        filters = query_payload.get("filters")
        if filters is not None and not isinstance(filters, list):
            raise ApiError(status_code=400, detail="source.query.filters must be a list")

        search = query_payload.get("search")
        if search is not None and not isinstance(search, str):
            raise ApiError(status_code=400, detail="source.query.search must be a string")

        actor_id = query_payload.get("actor_id")
        if actor_id is not None:
            try:
                actor_id = int(actor_id)
            except Exception:
                raise ApiError(
                    status_code=400, detail="source.query.actor_id must be an integer"
                )
    else:
        view_id = "default"
        sort = None
        filters = None
        search = None
        actor_id = None

    unique_asset_ids = sorted(set(asset_ids))
    query_total_count = None
    if query_payload:
        query_total_count = await count_assets_for_query(
            actor_id=actor_id,
            filters=filters,
            search=search,
        )

    # TODO Validate asset ids exist

    membership_key_id = get_metadata_id(COLLECTION_MEMBER)

    collection = await AssetCollection.create(
        name=payload.name,
        description=payload.description,
        source=payload.source,
        membership_key_id=membership_key_id,
        item_count=query_total_count if query_total_count is not None else len(unique_asset_ids),
        refresh_mode=refresh_mode,
    )

    if query_payload and query_total_count:
        actor = await ensure_user_editor()
        changeset = await Changeset.create(
            actor=actor,
            status=OpStatus.COMPLETED,
            note=f"collection:{collection.id} membership",
        )
        asset_table = Asset._meta.db_table
        metadata_table = Metadata._meta.db_table
        where_sql, filter_params = build_assets_where(
            actor_id=actor_id,
            filters=filters,
            search=search,
            extra_where=None,
        )
        conn = Tortoise.get_connection("default")
        # Single-shot insert to avoid pulling large query results into Python.
        insert_sql = f"""
        INSERT INTO {metadata_table} (
            asset_id,
            actor_id,
            changeset_id,
            metadata_key_id,
            value_type,
            value_collection_id,
            removed,
            confidence
        )
        SELECT
            a.id,
            ?,
            ?,
            ?,
            ?,
            ?,
            0,
            NULL
        FROM {asset_table} a
        {where_sql}
        """
        params = [
            actor.id,
            changeset.id,
            membership_key_id,
            int(MetadataType.COLLECTION),
            collection.id,
            *filter_params,
        ]
        await conn.execute_query(insert_sql, params)
    elif unique_asset_ids:
        actor = await ensure_user_editor()
        changeset = await Changeset.create(
            actor=actor,
            status=OpStatus.COMPLETED,
            note=f"collection:{collection.id} membership",
        )
        membership_entries = []
        for asset_id in unique_asset_ids:
            md = make_metadata(COLLECTION_MEMBER, collection.id, actor_id=actor.id)
            md.asset_id = asset_id
            md.changeset_id = changeset.id
            membership_entries.append(md)
            if len(membership_entries) >= 5000:
                await Metadata.bulk_create(membership_entries)
                membership_entries = []
        if membership_entries:
            await Metadata.bulk_create(membership_entries)

    return {"collection": collection.to_dict()}


async def get_collection_api(collection_id: int) -> dict[str, Any]:
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")
    return {"collection": collection.to_dict()}


async def update_collection_api(
    collection_id: int, payload: CollectionUpdate
) -> dict[str, Any]:
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")

    if payload.name:
        existing = await AssetCollection.get_or_none(name=payload.name)
        if existing and existing.id != collection.id:
            raise ApiError(status_code=400, detail="Collection name already exists")
        collection.name = payload.name

    if payload.description is not None:
        collection.description = payload.description

    if payload.refresh_mode:
        try:
            collection.refresh_mode = (
                CollectionRefreshMode(payload.refresh_mode)
                if isinstance(payload.refresh_mode, str)
                else payload.refresh_mode
            )
        except Exception:
            raise ApiError(
                status_code=400, detail="refresh_mode must be 'live' or 'on_demand'"
            )

    await collection.save()
    return {"collection": collection.to_dict()}


async def list_collection_assets_api(
    collection_id: int,
    view_id: str,
    offset: int,
    limit: int,
    sort: Optional[tuple[str, str]],
    columns: list[str] | None,
    search: Optional[str],
    filters: list[str] | None,
) -> dict[str, Any]:
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")

    try:
        view = get_view(view_id)
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")

    metadata_table = Metadata._meta.db_table
    membership_key_id = get_metadata_id(COLLECTION_MEMBER)
    extra_where = (
        "a.id IN ("
        "    WITH latest AS ("
        "        SELECT"
        "            m.asset_id,"
        "            m.removed,"
        "            ROW_NUMBER() OVER ("
        "                PARTITION BY m.asset_id, m.value_collection_id, m.actor_id"
        "                ORDER BY m.changeset_id DESC, m.id DESC"
        "            ) AS rn"
        f"        FROM {metadata_table} m"
        "        WHERE m.metadata_key_id = ?"
        "          AND m.value_collection_id = ?"
        "    )"
        "    SELECT asset_id FROM latest WHERE rn = 1 AND removed = 0"
        ")",
        [membership_key_id, collection.id],
    )

    try:
        return await list_assets_for_view(
            view,
            offset=offset,
            limit=limit,
            sort=sort,
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
            include_total=True,
            extra_where=extra_where,
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


async def delete_collection_api(collection_id: int) -> dict[str, Any]:
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")
    await collection.delete()
    return {"status": "deleted", "collection_id": collection_id}


@router.get("/collections")
async def list_collections():
    return await list_collections_api()


@router.post("/collections")
async def create_collection(request: Request):
    payload = CollectionCreate.model_validate(await request.json())
    return await create_collection_api(payload)


@router.get("/collections/{collection_id}")
async def get_collection(collection_id: int):
    return await get_collection_api(collection_id)


@router.patch("/collections/{collection_id}")
async def update_collection(collection_id: int, request: Request):
    payload = CollectionUpdate.model_validate(await request.json())
    return await update_collection_api(collection_id, payload)


@router.get("/collections/{collection_id}/assets")
async def list_collection_assets(
    collection_id: int,
    view_id: str = Query("default"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    sort: Optional[str] = Query(None),
    columns: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
):
    sort_tuple: tuple[str, str] | None = None
    if sort:
        if ":" in sort:
            col, direction = sort.split(":", 1)
        else:
            col, direction = sort, "asc"
        sort_tuple = (col, direction)
    return await list_collection_assets_api(
        collection_id=collection_id,
        view_id=view_id,
        offset=offset,
        limit=limit,
        sort=sort_tuple,
        columns=columns,
        search=search,
        filters=filters,
    )


@router.delete("/collections/{collection_id}")
async def delete_collection(collection_id: int):
    return await delete_collection_api(collection_id)
