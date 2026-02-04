from __future__ import annotations

from typing import Any, Protocol

from katalog.db.sqlspec.asset_collections import SqlspecAssetCollectionRepo
from katalog.models.assets import AssetCollection
from katalog.models.query import AssetQuery


class AssetCollectionRepo(Protocol):
    async def get_or_none(self, **filters: Any) -> AssetCollection | None: ...
    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[AssetCollection]: ...
    async def create(self, **fields: Any) -> AssetCollection: ...
    async def save(self, collection: AssetCollection) -> None: ...
    async def delete(self, collection_id: int) -> None: ...
    async def add_collection_members_for_query(
        self,
        *,
        collection_id: int,
        membership_key_id: int,
        actor_id: int,
        changeset_id: int,
        query: AssetQuery,
    ) -> int: ...


def get_asset_collection_repo() -> AssetCollectionRepo:
    return SqlspecAssetCollectionRepo()
