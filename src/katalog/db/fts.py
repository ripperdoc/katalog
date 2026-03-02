from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class FtsPoint:
    metadata_id: int
    text: str


@dataclass(frozen=True)
class FtsSearchHit:
    metadata_id: int
    asset_id: int
    metadata_key_id: int
    source_text: str
    rank: float | None


class FtsRepo(Protocol):
    async def is_ready(self) -> tuple[bool, str | None]: ...

    async def has_index_records(self, *, actor_id: int) -> bool: ...

    async def upsert_asset_points(
        self,
        *,
        asset_id: int,
        actor_id: int,
        metadata_key_ids: Sequence[int],
        points: Sequence[FtsPoint],
    ) -> int: ...

    async def search(
        self,
        *,
        actor_id: int,
        query_text: str,
        limit: int,
        offset: int = 0,
        asset_ids: Sequence[int] | None = None,
        metadata_key_ids: Sequence[int] | None = None,
    ) -> tuple[list[FtsSearchHit], int]: ...


def get_fts_repo() -> FtsRepo:
    from katalog.db.sqlspec.fts import SqlspecFtsRepo

    return SqlspecFtsRepo()

