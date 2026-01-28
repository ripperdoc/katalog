from typing import Any, Optional

from fastapi import APIRouter, Query, Request

from katalog.constants.metadata import MetadataKey
from katalog.db import (
    build_group_member_filter,
    list_assets_for_view,
    list_grouped_assets,
)
from katalog.editors.user_editor import ensure_user_editor
from katalog.models import Asset, Metadata, MetadataChanges, make_metadata
from katalog.models.views import get_view
from katalog.api.helpers import ApiError


router = APIRouter()


async def list_assets_api(actor_id: Optional[int] = None) -> dict[str, Any]:
    view = get_view("default")
    return await list_assets_for_view(view, actor_id=actor_id)


async def list_grouped_assets_api(
    group_by: str,
    group_value: Optional[str] = None,
    actor_id: Optional[int] = None,
    offset: int = 0,
    limit: int = 50,
    filters: list[str] | None = None,
    search: Optional[str] = None,
) -> dict[str, Any]:
    """
    Grouped asset listing:
    - Without group_value: returns group aggregates (row_kind='group').
    - With group_value: returns assets within that group (row_kind='asset').
    """

    view = get_view("default")

    if group_value is None:
        return await list_grouped_assets(
            view,
            group_by=group_by,
            actor_id=actor_id,
            offset=offset,
            limit=limit,
            filters=filters,
            search=search,
            include_total=True,
        )

    extra_where = build_group_member_filter(group_by, group_value)
    members = await list_assets_for_view(
        view,
        actor_id=actor_id,
        offset=offset,
        limit=limit,
        sort=None,
        filters=filters,
        columns=None,
        search=search,
        include_total=True,
        extra_where=extra_where,
    )
    # Tag rows so UI can distinguish assets returned via grouping.
    for item in members.get("items", []):
        item["row_kind"] = "asset"
        item["group_key"] = group_by
        item["group_value"] = group_value
    members["mode"] = "members"
    members["group_by"] = group_by
    members["group_value"] = group_value
    return members


async def create_asset_api() -> dict[str, Any]:
    raise NotImplementedError("Direct asset creation is not supported")


async def get_asset_api(asset_id: int) -> dict[str, Any]:
    asset = await Asset.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    metadata = await Metadata.for_asset(asset, include_removed=True)
    return {
        "asset": asset.to_dict(),
        "metadata": [m.to_dict() for m in metadata],
    }


async def manual_edit_asset_api(
    asset_id: int, payload: dict[str, Any]
) -> dict[str, Any]:
    changeset_id = payload.get("changeset_id")
    if changeset_id is None:
        raise ApiError(status_code=400, detail="changeset_id is required")

    from katalog.models import Changeset, OpStatus

    changeset = await Changeset.get_or_none(id=int(changeset_id))
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")

    asset = await Asset.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    actor = await ensure_user_editor()

    # Build metadata from payload (dict of key -> value)
    metadata_entries: list[Metadata] = []
    for key, value in payload.get("metadata", {}).items():
        try:
            mk = MetadataKey(key)
            md = make_metadata(mk, value, actor_id=actor.id)
        except Exception as exc:
            raise ApiError(status_code=400, detail=f"Invalid metadata {key}: {exc}")
        md.asset = asset
        md.changeset = changeset
        metadata_entries.append(md)

    # Apply changes
    loaded = await asset.load_metadata()
    changes = MetadataChanges(loaded=loaded, staged=metadata_entries)
    changed_keys = await changes.persist(asset=asset, changeset=changeset)

    return {
        "asset_id": asset_id,
        "changeset_id": changeset.id,
        "changed_keys": [str(k) for k in changed_keys],
    }


async def update_asset_api() -> dict[str, Any]:
    raise NotImplementedError()


@router.get("/assets")
async def list_assets(actor_id: Optional[int] = None):
    return await list_assets_api(actor_id=actor_id)


@router.get("/assets/grouped")
async def list_grouped_assets_endpoint(
    group_by: str = Query(
        ..., description="Grouping key, e.g. 'hash/md5' or 'a.actor_id'"
    ),
    group_value: Optional[str] = Query(
        None,
        description="When set, returns members of this group value instead of the group list.",
    ),
    actor_id: Optional[int] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    filters: list[str] | None = Query(None),
    search: Optional[str] = Query(None),
):
    return await list_grouped_assets_api(
        group_by=group_by,
        group_value=group_value,
        actor_id=actor_id,
        offset=offset,
        limit=limit,
        filters=filters,
        search=search,
    )


@router.post("/assets")
async def create_asset(request: Request):
    return await create_asset_api()


@router.get("/assets/{asset_id}")
async def get_asset(asset_id: int):
    return await get_asset_api(asset_id)


@router.post("/assets/{asset_id}/manual-edit")
async def manual_edit_asset(asset_id: int, request: Request):
    payload = await request.json()
    return await manual_edit_asset_api(asset_id, payload)


@router.patch("/assets/{asset_id}")
async def update_asset(asset_id: int):
    return await update_asset_api()
