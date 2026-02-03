from typing import Any, Optional, Sequence

from fastapi import APIRouter, Query, Request

from katalog.constants.metadata import MetadataKey
from katalog.db.assets import get_asset_repo
from katalog.editors.user_editor import ensure_user_editor
from katalog.models import Asset, Metadata, MetadataChanges, make_metadata
from katalog.models.views import get_view
from katalog.api.helpers import ApiError
from katalog.api.schemas import (
    AssetsListResponse,
    GroupedAssetsResponse,
    ManualEditResult,
)
from katalog.db.metadata import get_metadata_repo


router = APIRouter()


async def list_assets(actor_id: Optional[int] = None) -> AssetsListResponse:
    view = get_view("default")
    db = get_asset_repo()
    return await db.list_assets_for_view_db(view, actor_id=actor_id)


async def list_grouped_assets(
    group_by: str,
    group_value: Optional[str] = None,
    actor_id: Optional[int] = None,
    offset: int = 0,
    limit: int = 50,
    filters: list[str] | None = None,
    search: Optional[str] = None,
) -> GroupedAssetsResponse:
    """
    Grouped asset listing:
    - Without group_value: returns group aggregates (row_kind='group').
    - With group_value: returns assets within that group (row_kind='asset').
    """

    view = get_view("default")

    if group_value is None:
        db = get_asset_repo()
        return await db.list_grouped_assets_db(
            view,
            group_by=group_by,
            actor_id=actor_id,
            offset=offset,
            limit=limit,
            filters=filters,
            search=search,
            include_total=True,
        )

    db = get_asset_repo()
    extra_where = db.build_group_member_filter(group_by, group_value)
    members = await db.list_assets_for_view_db(
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
    items: list[dict[str, Any]] = []
    for item in members.items:
        row = item.model_dump(by_alias=True)
        row["row_kind"] = "asset"
        row["group_key"] = group_by
        row["group_value"] = group_value
        items.append(row)
    return GroupedAssetsResponse(
        mode="members",
        group_by=group_by,
        group_value=group_value,
        items=items,
        stats=members.stats.model_dump(mode="json"),
        pagination=members.pagination,
    )


async def create_asset() -> None:
    raise NotImplementedError("Direct asset creation is not supported")


async def get_asset(asset_id: int) -> tuple[Asset, Sequence[Metadata]]:
    db = get_asset_repo()
    asset = await db.get_or_none(id=asset_id)
    if asset is None:
        raise ApiError(status_code=404, detail="Asset not found")

    metadata = await db.load_metadata(asset, include_removed=True)
    return asset, metadata


async def manual_edit_asset(asset_id: int, payload: dict[str, Any]) -> ManualEditResult:
    changeset_id = payload.get("changeset_id")
    if changeset_id is None:
        raise ApiError(status_code=400, detail="changeset_id is required")

    from katalog.db.changesets import get_changeset_repo
    from katalog.models import OpStatus

    db = get_changeset_repo()
    changeset = await db.get_or_none(id=int(changeset_id))
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")

    db = get_asset_repo()
    asset = await db.get_or_none(id=asset_id)
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
        md.asset_id = asset.id
        md.changeset_id = changeset.id
        metadata_entries.append(md)

    # Apply changes
    loaded = await db.load_metadata(asset)
    changes = MetadataChanges(loaded=loaded, staged=metadata_entries)
    md_db = get_metadata_repo()
    changed_keys = await md_db.persist_changes(
        changes, asset=asset, changeset=changeset
    )

    return ManualEditResult(
        asset_id=asset_id,
        changeset_id=changeset.id,
        changed_keys=[str(k) for k in changed_keys],
    )


async def update_asset() -> None:
    raise NotImplementedError()


@router.get("/assets")
async def list_assets_rest(actor_id: Optional[int] = None):
    return await list_assets(actor_id=actor_id)


@router.get("/assets/grouped")
async def list_grouped_assets_rest(
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
    return await list_grouped_assets(
        group_by=group_by,
        group_value=group_value,
        actor_id=actor_id,
        offset=offset,
        limit=limit,
        filters=filters,
        search=search,
    )


@router.post("/assets")
async def create_asset_rest(request: Request):
    return await create_asset()


@router.get("/assets/{asset_id}")
async def get_asset_rest(asset_id: int):
    asset, metadata = await get_asset(asset_id)
    return {"asset": asset, "metadata": metadata}


@router.post("/assets/{asset_id}/manual-edit")
async def manual_edit_asset_rest(asset_id: int, request: Request):
    payload = await request.json()
    return await manual_edit_asset(asset_id, payload)


@router.patch("/assets/{asset_id}")
async def update_asset_rest(asset_id: int):
    return await update_asset()
