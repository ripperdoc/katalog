from __future__ import annotations

from typing import Any, Callable

from katalog.models import FileAccessor


class MemoryAccessor(FileAccessor):
    """In-memory FileAccessor implementation for deterministic hashing tests."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:  # noqa: ARG002
        if offset >= len(self._payload):
            return b""
        if length is None:
            return self._payload[offset:]
        return self._payload[offset : offset + length]
