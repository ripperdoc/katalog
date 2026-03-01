from __future__ import annotations

from typing import Any, Protocol

from katalog.db.sqlspec.system import SqlspecSystemRepo


class SystemRepo(Protocol):
    async def database_size_stats(self) -> dict[str, Any]: ...


def get_system_repo() -> SystemRepo:
    return SqlspecSystemRepo()
