from __future__ import annotations

from typing import Any, Protocol

from katalog.db.sqlspec.actors import SqlspecActorRepo
from katalog.models.core import Actor


class ActorRepo(Protocol):
    async def get_or_none(self, **filters: Any) -> Actor | None: ...
    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Actor]: ...
    async def create(self, **fields: Any) -> Actor: ...
    async def save(self, actor: Actor) -> None: ...


def get_actor_repo() -> ActorRepo:
    return SqlspecActorRepo()
