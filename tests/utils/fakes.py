from __future__ import annotations

from typing import Any, Callable

from katalog.models import FileAccessor


class DatabaseStub:
    """Test double that errors unless a method is explicitly overridden."""

    def __init__(self) -> None:
        self._overrides: dict[str, Callable[..., Any]] = {}

    def set_method(self, name: str, func: Callable[..., Any]) -> None:
        self._overrides[name] = func

    def __getattr__(self, name: str):
        if name in self._overrides:
            return self._overrides[name]
        raise AssertionError(f"Database method '{name}' was not mocked for this test")


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


__all__ = ["DatabaseStub", "MemoryAccessor"]
