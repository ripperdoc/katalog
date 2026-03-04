from __future__ import annotations

from pydantic import BaseModel

from katalog.models.query import (
    AssetsListResponse,
    ChangesetChangesResponse,
    ChangesetDiffResponse,
    GroupedAssetsResponse,
)


class RemoveAssetsResponse(BaseModel):
    """Response payload for collection asset removal."""
    removed: int
    skipped: int


class ManualEditResult(BaseModel):
    """Result payload for manual asset metadata edits."""
    asset_id: int
    changeset_id: int
    changed_keys: list[str]
