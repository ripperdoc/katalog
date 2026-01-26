from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from katalog.constants.metadata import COLLECTION_MEMBER, get_metadata_id
from katalog.db import list_assets_for_view
from katalog.models import (
    AssetCollection,
    Changeset,
    Metadata,
    CollectionRefreshMode,
    OpStatus,
    make_metadata,
)
from katalog.models.views import get_view
from katalog.editors.user_editor import ensure_user_editor

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


@router.get("/collections")
async def list_collections():
    collections = await AssetCollection.all().order_by("-created_at")
    result = []
    for col in collections:
        result.append(col.to_dict())
    return {"collections": result}


@router.post("/collections")
async def create_collection(request: Request):
    payload = CollectionCreate.model_validate(await request.json())

    try:
        asset_ids = [int(a) for a in payload.asset_ids]
    except Exception:
        raise HTTPException(status_code=400, detail="asset_ids must be integers")

    unique_asset_ids = sorted(set(asset_ids))

    existing = await AssetCollection.get_or_none(name=payload.name)
    if existing:
        raise HTTPException(status_code=400, detail="Collection name already exists")

    refresh_mode = payload.refresh_mode or CollectionRefreshMode.ON_DEMAND
    if isinstance(refresh_mode, str):
        try:
            refresh_mode = CollectionRefreshMode(refresh_mode)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="refresh_mode must be 'live' or 'on_demand'",
            )

    # TODO Validate asset ids exist

    membership_key_id = get_metadata_id(COLLECTION_MEMBER)

    collection = await AssetCollection.create(
        name=payload.name,
        description=payload.description,
        source=payload.source,
        membership_key_id=membership_key_id,
        item_count=len(unique_asset_ids),
        refresh_mode=refresh_mode,
    )

    if unique_asset_ids:
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
        await Metadata.bulk_create(membership_entries)

    return {"collection": collection.to_dict()}


@router.get("/collections/{collection_id}")
async def get_collection(collection_id: int):
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")
    return {"collection": collection.to_dict()}


@router.patch("/collections/{collection_id}")
async def update_collection(collection_id: int, request: Request):
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")

    payload = CollectionUpdate.model_validate(await request.json())

    if payload.name:
        existing = await AssetCollection.get_or_none(name=payload.name)
        if existing and existing.id != collection.id:
            raise HTTPException(
                status_code=400, detail="Collection name already exists"
            )
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
            raise HTTPException(
                status_code=400, detail="refresh_mode must be 'live' or 'on_demand'"
            )

    await collection.save()
    return {"collection": collection.to_dict()}


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
    collection = await AssetCollection.get_or_none(id=collection_id)
    if collection is None:
        raise HTTPException(status_code=404, detail="Collection not found")

    try:
        view = get_view(view_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="View not found")

    sort_tuple: tuple[str, str] | None = None
    if sort:
        if ":" in sort:
            col, direction = sort.split(":", 1)
        else:
            col, direction = sort, "asc"
        sort_tuple = (col, direction)

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
            sort=sort_tuple,
            filters=filters,
            columns=set(columns) if columns else None,
            search=search,
            include_total=True,
            extra_where=extra_where,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
