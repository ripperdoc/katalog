from __future__ import annotations

import re
from typing import Iterable


_HIDDEN_BASENAMES = {
    "thumbs.db",
    "ehthumbs.db",
    "desktop.ini",
    "$recycle.bin",
    "__macosx",
}


def should_hide_path(path: str | None, mime_type: str | None) -> bool:
    """Return True when a path should be treated as hidden.

    The MIME type is accepted for future rule expansion, but the current rules
    only inspect path components.
    """
    _ = mime_type
    if not path:
        return False
    normalized = path.strip()
    if not normalized:
        return False
    for part in _split_path_parts(normalized):
        if part.startswith("."):
            return True
        lowered = part.lower()
        if lowered in _HIDDEN_BASENAMES:
            return True
        if lowered.startswith("~$"):
            return True
    return False


def _split_path_parts(path: str) -> Iterable[str]:
    trimmed = path.strip().strip("/\\")
    if not trimmed:
        return []
    parts = re.split(r"[\\/]+", trimmed)
    return [part for part in parts if part not in {"", ".", ".."}]
