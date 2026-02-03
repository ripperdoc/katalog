from __future__ import annotations

from typing import Any, Iterable, Mapping, Protocol, TYPE_CHECKING

from katalog.db.sqlspec.changesets import SqlspecChangesetRepo
from katalog.models.core import Actor, Changeset, OpStatus


class ChangesetRepo(Protocol):
    async def get_or_none(self, **filters: Any) -> Changeset | None: ...
    async def get(self, **filters: Any) -> Changeset: ...
    async def list_rows(
        self,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        **filters: Any,
    ) -> list[Changeset]: ...
    async def list_for_actor(self, actor_id: int) -> list[Changeset]: ...
    async def begin(
        self,
        *,
        status: OpStatus = OpStatus.IN_PROGRESS,
        data: Mapping[str, Any] | None = None,
        actors: Iterable[Actor] | None = None,
        message: str | None = None,
    ) -> Changeset: ...
    async def create(
        self,
        *,
        id: int,
        status: OpStatus,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> Changeset: ...
    async def create_auto(
        self,
        *,
        status: OpStatus,
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> Changeset: ...
    async def add_actors(self, changeset: Changeset, actors: Iterable[Actor]) -> None: ...
    async def load_actor_ids(self, changeset: Changeset) -> list[int]: ...
    async def save(
        self, changeset: Changeset, *, update_data: dict[str, Any] | None = None
    ) -> None: ...
    async def delete(self, changeset: Changeset) -> None: ...
    async def list_changeset_metadata_changes(
        self,
        changeset_id: int,
        *,
        offset: int = 0,
        limit: int = 200,
        include_total: bool = True,
    ) -> ChangesetChangesResponse: ...


def get_changeset_repo() -> ChangesetRepo:
    return SqlspecChangesetRepo()


if TYPE_CHECKING:
    from katalog.models.query import ChangesetChangesResponse
