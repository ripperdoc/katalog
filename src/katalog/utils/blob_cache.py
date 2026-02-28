from __future__ import annotations

from pathlib import Path
import re
from typing import Final

from diskcache import Cache
from loguru import logger

from katalog.config import current_app_context, current_workspace

_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]+$")
_MAX_CACHE_BYTES: Final[int] = 2 * 1024 * 1024 * 1024  # 2 GiB


def _cache_state() -> tuple[dict[Path, Cache], set[Path]]:
    state = current_app_context().state
    cache_by_dir = state.get("blob_cache_by_dir")
    init_failed = state.get("blob_cache_init_failed")
    if cache_by_dir is None:
        cache_by_dir = {}
        state["blob_cache_by_dir"] = cache_by_dir
    if init_failed is None:
        init_failed = set()
        state["blob_cache_init_failed"] = init_failed
    return cache_by_dir, init_failed


def _cache_dir() -> Path:
    workspace = current_workspace()
    return workspace / "cache" / "blobs"


def get_blob_cache() -> Cache | None:
    cache_by_dir, init_failed = _cache_state()
    cache_dir = _cache_dir()
    cached = cache_by_dir.get(cache_dir)
    if cached is not None:
        return cached
    if cache_dir in init_failed:
        return None
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache = Cache(str(cache_dir), size_limit=_MAX_CACHE_BYTES)
        cache_by_dir[cache_dir] = cache
        return cache
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to initialize blob cache at {path}: {err}", path=cache_dir, err=exc)
        init_failed.add(cache_dir)
        return None


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


def close_blob_caches() -> None:
    cache_by_dir, init_failed = _cache_state()
    targets = list(cache_by_dir.keys())

    for cache_dir in targets:
        cache = cache_by_dir.pop(cache_dir, None)
        if cache is None:
            continue
        try:
            cache.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Blob cache close failed path={path}: {err}",
                path=cache_dir,
                err=exc,
            )
        init_failed.discard(cache_dir)
