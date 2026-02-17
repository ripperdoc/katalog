from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


@dataclass(frozen=True)
class SidecarDescriptor:
    suffix: str
    kind: str
    target_name: str


DEFAULT_SIDECARS: tuple[tuple[str, str], ...] = (
    (".truth.md", "truth_markdown"),
    (".queries.yml", "queries_yaml"),
    (".queries.yaml", "queries_yaml"),
    (".summary.md", "summary_markdown"),
)


def detect_sidecar(path_or_name: str) -> SidecarDescriptor | None:
    name = _extract_name(path_or_name)
    for suffix, kind in DEFAULT_SIDECARS:
        if not name.endswith(suffix):
            continue
        target_name = name[: -len(suffix)]
        if not target_name:
            return None
        return SidecarDescriptor(
            suffix=suffix,
            kind=kind,
            target_name=target_name,
        )
    return None


def is_sidecar(path_or_name: str) -> bool:
    return detect_sidecar(path_or_name) is not None


def parse_sidecar_payload(path_or_name: str, *, text: str) -> dict[str, Any] | None:
    descriptor = detect_sidecar(path_or_name)
    if descriptor is None:
        return None
    if descriptor.kind == "truth_markdown":
        return {"truth_text": text}
    if descriptor.kind == "summary_markdown":
        return {"summary_text": text}
    if descriptor.kind == "queries_yaml":
        parsed = _parse_yaml_or_json(text)
        if parsed is None:
            return None
        return {"queries": parsed}
    return None


def _extract_name(path_or_name: str) -> str:
    raw = (path_or_name or "").strip()
    if not raw:
        return ""
    if "/" in raw:
        return PurePosixPath(raw).name
    return Path(raw).name


def _parse_yaml_or_json(text: str) -> Any | None:
    try:
        import yaml
    except Exception:
        yaml = None
    if yaml is not None:
        try:
            return yaml.safe_load(text)
        except Exception:
            pass
    try:
        return json.loads(text)
    except Exception:
        return None
