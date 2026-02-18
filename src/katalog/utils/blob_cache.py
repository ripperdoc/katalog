from __future__ import annotations

from pathlib import Path
import re
from typing import Final

from diskcache import Cache
from loguru import logger

from katalog.config import WORKSPACE

_CACHE: Cache | None = None
_CACHE_INIT_ATTEMPTED = False
_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]+$")
_MAX_CACHE_BYTES: Final[int] = 2 * 1024 * 1024 * 1024  # 2 GiB


def _cache_dir() -> Path | None:
    if WORKSPACE is None:
        return None
    return WORKSPACE / "cache" / "blobs"


def get_blob_cache() -> Cache | None:
    global _CACHE, _CACHE_INIT_ATTEMPTED
    if _CACHE is not None:
        return _CACHE
    if _CACHE_INIT_ATTEMPTED:
        return None
    _CACHE_INIT_ATTEMPTED = True

    cache_dir = _cache_dir()
    if cache_dir is None:
        return None
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        _CACHE = Cache(str(cache_dir), size_limit=_MAX_CACHE_BYTES)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to initialize blob cache at {path}: {err}", path=cache_dir, err=exc)
        _CACHE = None
    return _CACHE


def _cache_key(hash_type: str, digest: str) -> str | None:
    normalized_type = (hash_type or "").strip().lower()
    normalized_digest = (digest or "").strip().lower()
    if not normalized_type or not normalized_digest:
        return None
    if not _HEX_RE.match(normalized_digest):
        return None
    return f"{normalized_type}:{normalized_digest}"


def get_cached_blob(*, hash_type: str, digest: str) -> bytes | None:
    key = _cache_key(hash_type, digest)
    if key is None:
        return None
    cache = get_blob_cache()
    if cache is None:
        return None
    try:
        value = cache.get(key)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Blob cache read failed key={key}: {err}", key=key, err=exc)
        return None
    if isinstance(value, bytes):
        return value
    return None


def put_cached_blob(*, hash_type: str, digest: str, data: bytes) -> None:
    key = _cache_key(hash_type, digest)
    if key is None:
        return
    cache = get_blob_cache()
    if cache is None:
        return
    try:
        cache.set(key, data)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Blob cache write failed key={key}: {err}", key=key, err=exc)
