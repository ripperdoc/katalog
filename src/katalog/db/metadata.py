from __future__ import annotations

from typing import Any, Protocol, Sequence, TYPE_CHECKING

from katalog.db.sqlspec.metadata import SqlspecMetadataRepo
from katalog.db.sqlspec.query_metadata_registry import (
    setup_db,
    sync_config_db,
    sync_metadata_registry,
)
from katalog.models.metadata import Metadata


class MetadataRepo(Protocol):
    async def for_asset(
        self,
        asset: Asset | int,
        *,
        include_removed: bool = False,
        session: Any | None = None,
    ) -> Sequence[Metadata]: ...
    async def for_assets(
        self,
        asset_ids: Sequence[int],
        *,
        include_removed: bool = False,
        session: Any | None = None,
    ) -> dict[int, list[Metadata]]: ...

    async def bulk_create(
        self, metadata: Sequence[Metadata], *, session: Any | None = None
    ) -> None: ...
    async def persist_changes(
        self,
        changes: MetadataChanges,
        *,
        changeset: Any,
        existing_metadata: Sequence[Metadata] | None = None,
        session: Any | None = None,
    ) -> set[MetadataKey]: ...
    async def persist_changes_batch(
        self,
        changeset: Any,
        changes_list: Sequence[MetadataChanges],
        existing_metadata_by_asset: dict[int, list[Metadata]],
        *,
        session: Any | None = None,
    ) -> tuple[int, int, int]: ...
    async def list_active_collection_asset_ids(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
        asset_ids: Sequence[int],
    ) -> list[int]: ...
    async def list_removed_collection_asset_ids(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
        actor_id: int,
        changeset_id: int,
        asset_ids: Sequence[int],
    ) -> set[int]: ...
    async def count_active_collection_assets(
        self,
        *,
        membership_key_id: int,
        collection_id: int,
    ) -> int: ...


def get_metadata_repo() -> MetadataRepo:
    return SqlspecMetadataRepo()


if TYPE_CHECKING:
    from katalog.constants.metadata import MetadataKey
    from katalog.models.assets import Asset
    from katalog.models.metadata import MetadataChanges
