from __future__ import annotations

from pydantic import BaseModel

from katalog.models.query import (
    AssetsListResponse,
    ChangesetChangesResponse,
    GroupedAssetsResponse,
)


class RemoveAssetsResponse(BaseModel):
    removed: int
    skipped: int


class ManualEditResult(BaseModel):
    asset_id: int
    changeset_id: int
    changed_keys: list[str]
