from __future__ import annotations

from typing import Any, Protocol, Sequence, TYPE_CHECKING

from katalog.db.sqlspec.assets import SqlspecAssetRepo
from katalog.models.assets import Asset
from katalog.models.query import AssetQuery


class AssetRepo(Protocol):
    async def get_or_none(self, **filters: Any) -> Asset | None: ...
    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Asset]: ...
    async def save_record(
        self,
        asset: Asset,
        *,
        changeset: Any,
        actor: Any | None,
        session: Any | None = None,
    ) -> bool: ...
    async def load_metadata(
        self,
        asset: Asset,
        *,
        include_removed: bool = True,
        session: Any | None = None,
    ) -> Sequence[Metadata]: ...
    async def mark_unseen_as_lost(
        self,
        *,
        changeset: Any,
        actor_ids: Sequence[int],
        seen_asset_ids: Sequence[int] | None = None,
    ) -> int: ...
    async def count_assets_for_query(
        self,
        *,
        query: AssetQuery,
        extra_where: tuple[str, list[Any]] | None = None,
    ) -> int: ...
    async def list_asset_ids_for_query(
        self,
        *,
        query: AssetQuery,
        extra_where: tuple[str, list[Any]] | None = None,
    ) -> list[int]: ...
    async def list_assets_for_view_db(
        self,
        view: ViewSpec,
        *,
        query: AssetQuery,
        include_total: bool = True,
        extra_where: tuple[str, list[Any]] | None = None,
    ) -> AssetsListResponse: ...
    async def list_grouped_assets_db(
        self,
        view: ViewSpec,
        *,
        group_by: str,
        query: AssetQuery,
        include_total: bool = True,
    ) -> GroupedAssetsResponse: ...
    def build_group_member_filter(
        self, group_by: str, group_value: str
    ) -> tuple[str, list[Any]]: ...
    def build_collection_membership_filter(
        self, *, membership_key_id: int, collection_id: int
    ) -> tuple[str, list[Any]]: ...


def get_asset_repo() -> AssetRepo:
    return SqlspecAssetRepo()


if TYPE_CHECKING:
    from katalog.models.metadata import Metadata
    from katalog.models.query import AssetsListResponse, GroupedAssetsResponse
    from katalog.models.views import ViewSpec
