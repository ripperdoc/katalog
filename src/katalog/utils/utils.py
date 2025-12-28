import asyncio
import base64
import importlib
from datetime import datetime, timezone
import json
from typing import Any, Mapping, Optional

from loguru import logger


def import_plugin_class(
    package_path: str, *, default_package: str = "katalog"
) -> type[Any]:
    """Dynamically import a plugin class, allowing for short module paths."""

    module_name, class_name = package_path.rsplit(".", 1)
    module_candidates = [module_name]
    if default_package and not module_name.startswith(f"{default_package}."):
        module_candidates.append(f"{default_package}.{module_name}")
    last_error: ModuleNotFoundError | None = None
    for candidate in module_candidates:
        try:
            module = importlib.import_module(candidate)
        except ModuleNotFoundError as exc:
            last_error = exc
            if candidate.startswith(f"{default_package}."):
                raise
            continue
        try:
            return getattr(module, class_name)
        except AttributeError as exc:
            raise ImportError(
                f"Unable to locate class '{class_name}' in module '{candidate}'"
            ) from exc
    if last_error:
        raise last_error
    raise ModuleNotFoundError(
        f"Unable to import module '{module_name}' for '{package_path}'"
    )


def timestamp_to_utc(ts: float | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def parse_google_drive_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """
    Parse a Google Drive ISO8601 date string (e.g. '2017-10-24T15:01:04.000Z') to a Python datetime (UTC).
    Returns None if input is None or invalid.
    """
    if not dt_str:
        return None
    try:
        # Google returns ISO8601 with 'Z' for UTC
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        try:
            # Fallback: sometimes no microseconds
            return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            return None


def _encode_cursor(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str) -> dict[str, Any]:
    padding = "=" * (-len(cursor) % 4)
    raw = base64.urlsafe_b64decode(f"{cursor}{padding}")
    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        raise ValueError("cursor must decode to an object")
    return decoded


def orm(cls: type) -> str:
    """Method that ensures we get the correct name from a Tortoise ORM model class."""
    # return f"{cls.__module__}.{cls.__qualname__}"
    return f"models.{cls.__qualname__}"


def fqn(cls: type) -> str:
    """Get the fully qualified name of a class."""
    return f"{cls.__module__}.{cls.__qualname__}"
