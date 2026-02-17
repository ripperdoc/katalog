from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class VectorPoint:
    metadata_id: int
    vector: list[float]


@dataclass(frozen=True)
class VectorSearchHit:
    point_id: int
    asset_id: int
    metadata_key_id: int
    source_text: str
    distance: float
    metadata_id: int


class VectorRepo(Protocol):
    async def is_ready(self) -> tuple[bool, str | None]: ...

    async def upsert_asset_points(
        self,
        *,
        asset_id: int,
        actor_id: int,
        dim: int,
        metadata_key_ids: Sequence[int],
        points: Sequence[VectorPoint],
    ) -> int: ...

    async def search(
        self,
        *,
        actor_id: int,
        dim: int,
        query_vector: Sequence[float],
        limit: int,
        asset_ids: Sequence[int] | None = None,
    ) -> list[VectorSearchHit]: ...


def get_vector_repo() -> VectorRepo:
    from katalog.db.sqlspec.vectors import SqlspecVectorRepo

    return SqlspecVectorRepo()
