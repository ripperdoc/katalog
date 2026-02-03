import base64
import importlib
from datetime import UTC, datetime, timedelta, timezone
import json
from fnmatch import fnmatch
from typing import Any, Iterable, Mapping, Optional

from pydantic import BaseModel, ConfigDict


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


def parse_datetime_utc(
    value: datetime | str | None, *, strict: bool = False
) -> datetime | None:
    """Parse an ISO8601 datetime/date into a timezone-aware UTC datetime.

    Accepts:
    - `datetime` (naive treated as UTC)
    - ISO8601 `str` like '2025-01-01', '2025-01-01T12:34:56', '2025-01-01T12:34:56Z'
    - `None`

    If `strict=False`, returns None for invalid values.
    If `strict=True`, raises ValueError/TypeError for invalid values.
    """
    if value is None:
        return None

    dt: datetime
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        # Accept ISO8601 with 'Z' suffix.
        raw = raw.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            if strict:
                raise
            return None
    else:
        if strict:
            raise TypeError(
                f"Invalid datetime type: {type(value)!r}. Expected datetime, str, or None."
            )
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


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


def coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        value = stripped
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_glob_patterns(raw: list[str] | str | None) -> list[str]:
    """Return a cleaned list of glob patterns."""
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [p for p in raw if isinstance(p, str) and p.strip()]


def match_paths(
    *,
    paths: Iterable[str],
    include: list[str],
    exclude: list[str],
) -> bool:
    """
    Return True if any of the `paths` pass include/exclude rules.
    - Exclude patterns take priority (if any match, reject).
    - If include is empty, everything not excluded matches.
    - If include is non-empty, require at least one match.
    """
    path_list = list(paths)
    if exclude and any(
        fnmatch(path, pattern) for path in path_list for pattern in exclude
    ):
        return False
    if not include:
        return True
    return any(fnmatch(path, pattern) for path in path_list for pattern in include)


class TimeSlice(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: Optional[datetime]
    end: Optional[datetime]

    def split(
        self, start: Optional[datetime] = None, end: Optional[datetime] = None
    ) -> tuple["TimeSlice", "TimeSlice"]:
        # Use provided bounds or fall back to the TimeSlice's values
        start = start or self.start
        end = end or self.end

        # For calculation, substitute reasonable finite bounds when open-ended:
        # - if end is open, treat it as 'now'
        # - if start is open, treat it as 10 years before end
        now = datetime.now(UTC)
        calc_end = end if end is not None else now
        calc_start = (
            start if start is not None else (calc_end - timedelta(days=10 * 365))
        )

        if calc_start >= calc_end:
            raise ValueError("TimeSlice start must be before end to split")

        # Compute the midpoint at maximum resolution
        split_at = calc_start + (calc_end - calc_start) / 2

        # Preserve open-endedness in the returned slices: use original None values
        first = TimeSlice(start=start, end=split_at)
        second = TimeSlice(start=split_at, end=end)
        return first, second

    def splittable(self) -> bool:
        """Return True if this TimeSlice is larger than 1 hour (e.g. can be meaningfully split)"""
        # NOTE to avoid getting into very small slices that can cause unexpected behaviour
        if self.start is None or self.end is None:
            return True
        return (self.end - self.start) > timedelta(hours=1)

    def __repr__(self) -> str:
        start = self.start.date().isoformat() if self.start else "begin"
        end = self.end.date().isoformat() if self.end else "end"
        return f"{start}->{end}"

    @classmethod
    def from_dict(cls, data: Any) -> "TimeSlice":
        return cls.model_validate(data)
