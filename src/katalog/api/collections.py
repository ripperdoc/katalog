from typing import Any, Optional

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field, ValidationError
from loguru import logger

from katalog.constants.metadata import COLLECTION_MEMBER, get_metadata_id
from katalog.models import (
    AssetCollection,
    CollectionRefreshMode,
    OpStatus,
    make_metadata,
)
from katalog.models.query import AssetFilter, AssetQuery
from katalog.models.views import get_view
from katalog.editors.user_editor import ensure_user_editor
from katalog.api.helpers import ApiError
from katalog.api.query_utils import build_asset_query
from katalog.api.schemas import AssetsListResponse, RemoveAssetsResponse
from katalog.db.asset_collections import get_asset_collection_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo

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


class CollectionRemoveAssets(BaseModel):
    asset_ids: list[int] = Field(default_factory=list)
    changeset_id: int


async def list_collections() -> list[AssetCollection]:
    db = get_asset_collection_repo()
    collections = await db.list_rows(order_by="created_at DESC")
    return collections


async def create_collection(payload: CollectionCreate) -> AssetCollection:
    db = get_asset_collection_repo()
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

    existing = await db.get_or_none(name=payload.name)
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
        try:
            query = AssetQuery.model_validate(query_payload)
        except ValidationError as exc:
            raise ApiError(
                status_code=400,
                detail={"message": "Invalid source.query", "errors": exc.errors()},
            ) from exc
    else:
        query = AssetQuery.model_validate({"view_id": "default"})

    unique_asset_ids = sorted(set(asset_ids))
    query_total_count = None
    if query_payload:
        asset_db = get_asset_repo()
        query_total_count = await asset_db.count_assets_for_query(query=query)

    # TODO Validate asset ids exist

    membership_key_id = get_metadata_id(COLLECTION_MEMBER)

    collection = await db.create(
        name=payload.name,
        description=payload.description,
        source=payload.source,
        membership_key_id=membership_key_id,
        asset_count=query_total_count
        if query_total_count is not None
        else len(unique_asset_ids),
        refresh_mode=refresh_mode,
    )

    collection_id_value = collection.id
    if collection_id_value is None:
        raise ApiError(status_code=409, detail="Collection id is missing")

    if query_payload and query_total_count:
        actor = await ensure_user_editor()
        if actor.id is None:
            raise ApiError(status_code=409, detail="Actor id is missing")
        changeset_db = get_changeset_repo()
        changeset = await changeset_db.create_auto(
            status=OpStatus.COMPLETED,
            message=f"Created collection {collection_id_value}",
        )
        await changeset_db.add_actors(changeset, [actor])
        await db.add_collection_members_for_query(
            collection_id=collection_id_value,
            membership_key_id=membership_key_id,
            actor_id=actor.id,
            changeset_id=changeset.id,
            query=query,
        )
    elif unique_asset_ids:
        actor = await ensure_user_editor()
        if actor.id is None:
            raise ApiError(status_code=409, detail="Actor id is missing")
        changeset_db = get_changeset_repo()
        changeset = await changeset_db.create_auto(
            status=OpStatus.COMPLETED,
            message=f"Created collection {collection_id_value}",
        )
        await changeset_db.add_actors(changeset, [actor])
        membership_entries = []
        for asset_id in unique_asset_ids:
            md = make_metadata(COLLECTION_MEMBER, collection_id_value, actor_id=actor.id)
            md.asset_id = asset_id
            md.changeset_id = changeset.id
            membership_entries.append(md)
            if len(membership_entries) >= 5000:
                md_db = get_metadata_repo()
                await md_db.bulk_create(membership_entries)
                membership_entries = []
        if membership_entries:
            md_db = get_metadata_repo()
            await md_db.bulk_create(membership_entries)

    return collection


async def get_collection(collection_id: int) -> AssetCollection:
    db = get_asset_collection_repo()
    collection = await db.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")
    collection_id_value = collection.id
    if collection_id_value is None:
        raise ApiError(status_code=409, detail="Collection id is missing")
    return collection


async def update_collection(
    collection_id: int, payload: CollectionUpdate
) -> AssetCollection:
    db = get_asset_collection_repo()
    collection = await db.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")

    if payload.name:
        existing = await db.get_or_none(name=payload.name)
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

    await db.save(collection)
    return collection


async def list_collection_assets(
    collection_id: int,
    query: AssetQuery,
) -> AssetsListResponse:
    db = get_asset_collection_repo()
    collection = await db.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")

    try:
        view = get_view(query.view_id or "default")
    except KeyError:
        raise ApiError(status_code=404, detail="View not found")

    collection_id_value = collection.id
    if collection_id_value is None:
        raise ApiError(status_code=409, detail="Collection id is missing")
    collection_filter = AssetFilter(
        key=str(COLLECTION_MEMBER),
        op="equals",
        value=str(collection_id_value),
    )

    try:
        filters = list(query.filters or [])
        filters.append(collection_filter)
        query_db = query.model_copy(update={"filters": filters})
        # TODO: metadata_actor_ids support is intentionally skipped for now.
        asset_db = get_asset_repo()
        return await asset_db.list_assets_for_view_db(
            view,
            query=query_db,
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


async def delete_collection(collection_id: int) -> dict[str, int | str]:
    db = get_asset_collection_repo()
    collection = await db.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")
    collection_id_value = collection.id
    if collection_id_value is None:
        raise ApiError(status_code=409, detail="Collection id is missing")
    await db.delete(collection_id_value)
    return {"status": "deleted", "collection_id": collection_id}


async def remove_collection_assets(
    collection_id: int, payload: CollectionRemoveAssets
) -> RemoveAssetsResponse:
    db = get_asset_collection_repo()
    collection = await db.get_or_none(id=collection_id)
    if collection is None:
        raise ApiError(status_code=404, detail="Collection not found")

    try:
        asset_ids = sorted({int(a) for a in payload.asset_ids})
    except Exception:
        raise ApiError(status_code=400, detail="asset_ids must be integers")

    if not asset_ids:
        return RemoveAssetsResponse(removed=0, skipped=0)

    changeset_db = get_changeset_repo()
    changeset = await changeset_db.get_or_none(id=payload.changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")

    if not isinstance(changeset.data, dict) or not changeset.data.get("manual"):
        raise ApiError(
            status_code=409,
            detail="Changeset must be a manual edit",
        )

    collection_id_value = collection.id
    if collection_id_value is None:
        raise ApiError(status_code=409, detail="Collection id is missing")

    actor = await ensure_user_editor()
    if actor.id is None:
        raise ApiError(status_code=409, detail="Actor id is missing")
    await changeset_db.add_actors(changeset, [actor])

    membership_key_id = get_metadata_id(COLLECTION_MEMBER)
    md_db = get_metadata_repo()
    active_asset_ids = await md_db.list_active_collection_asset_ids(
        membership_key_id=membership_key_id,
        collection_id=collection_id_value,
        asset_ids=asset_ids,
    )

    if not active_asset_ids:
        return RemoveAssetsResponse(removed=0, skipped=len(asset_ids))

    # Avoid inserting duplicate removals within the same changeset.
    already_removed = await md_db.list_removed_collection_asset_ids(
        membership_key_id=membership_key_id,
        collection_id=collection_id_value,
        actor_id=actor.id,
        changeset_id=changeset.id,
        asset_ids=active_asset_ids,
    )
    active_asset_ids = [aid for aid in active_asset_ids if aid not in already_removed]
    if not active_asset_ids:
        return RemoveAssetsResponse(removed=0, skipped=len(asset_ids))

    membership_entries = []
    for asset_id in active_asset_ids:
        md = make_metadata(
            COLLECTION_MEMBER,
            collection_id_value,
            actor_id=actor.id,
            removed=True,
        )
        md.asset_id = asset_id
        md.changeset_id = changeset.id
        membership_entries.append(md)
        if len(membership_entries) >= 5000:
            md_db = get_metadata_repo()
            await md_db.bulk_create(membership_entries)
            membership_entries = []
    if membership_entries:
        md_db = get_metadata_repo()
        await md_db.bulk_create(membership_entries)

    current_count = await md_db.count_active_collection_assets(
        membership_key_id=membership_key_id,
        collection_id=collection_id_value,
    )
    collection.asset_count = current_count
    await db.save(collection)

    logger.bind(changeset_id=changeset.id).info(
        "Removed {count} assets from collection {collection_id}",
        count=len(active_asset_ids),
        collection_id=collection_id_value,
    )

    return RemoveAssetsResponse(
        removed=len(active_asset_ids),
        skipped=len(asset_ids) - len(active_asset_ids),
    )


@router.get("/collections")
async def list_collections_rest():
    collections = await list_collections()
    return {"collections": collections}


@router.post("/collections")
async def create_collection_rest(request: Request):
    payload = CollectionCreate.model_validate(await request.json())
    collection = await create_collection(payload)
    return {"collection": collection}


@router.get("/collections/{collection_id}")
async def get_collection_rest(collection_id: int):
    collection = await get_collection(collection_id)
    return {"collection": collection}


@router.patch("/collections/{collection_id}")
async def update_collection_rest(collection_id: int, request: Request):
    payload = CollectionUpdate.model_validate(await request.json())
    collection = await update_collection(collection_id, payload)
    return {"collection": collection}


@router.get("/collections/{collection_id}/assets")
async def list_collection_assets_rest(
    collection_id: int,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    view_id: str = Query("default"),
    sort: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
    filters: list[str] | None = Query(None),
    metadata_actor_ids: list[int] | None = Query(None),
    metadata_include_removed: bool = Query(False),
    metadata_aggregation: Optional[str] = Query(None),
    metadata_include_counts: bool = Query(True),
):
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
    except Exception as exc:
        raise ApiError(status_code=400, detail=str(exc)) from exc
    return await list_collection_assets(
        collection_id=collection_id,
        query=query,
    )


@router.delete("/collections/{collection_id}")
async def delete_collection_rest(collection_id: int):
    return await delete_collection(collection_id)


@router.post("/collections/{collection_id}/remove")
async def remove_collection_assets_rest(collection_id: int, request: Request):
    payload = CollectionRemoveAssets.model_validate(await request.json())
    return await remove_collection_assets(collection_id, payload)
