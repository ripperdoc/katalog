from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from loguru import logger

from katalog.constants.metadata import DOC_SUMMARY, EVAL_QUERIES, EVAL_TRUTH_TEXT
from katalog.sources.base import AssetScanResult
from katalog.models import Actor, Asset


@dataclass(frozen=True)
class SidecarSpec:
    name: str
    suffix: str


@dataclass
class SidecarRecord:
    path: str
    spec: SidecarSpec
    payload: Any
    target_stem: str


@dataclass
class ResolvedSidecar:
    sidecar: SidecarRecord
    asset: Asset


DEFAULT_SIDECAR_SPECS: tuple[SidecarSpec, ...] = (
    SidecarSpec(name="truth_markdown", suffix=".truth.md"),
    SidecarSpec(name="queries_yaml", suffix=".queries.yml"),
    SidecarSpec(name="queries_yaml", suffix=".queries.yaml"),
    SidecarSpec(name="summary_markdown", suffix=".summary.md"),
)

_SIDECAR_SUFFIXES: tuple[str, ...] = tuple(spec.suffix for spec in DEFAULT_SIDECAR_SPECS)


@dataclass
class SidecarResolver:
    """Minimal resolver for matching sidecars to discovered assets.

    Strategy:
    - Detect sidecars by configured suffixes.
    - Match by filename stem (base name before sidecar suffix).
    - If no asset exists yet, keep sidecar in a pending queue.
    - Re-try pending sidecars whenever new assets are registered.
    """

    specs: tuple[SidecarSpec, ...] = DEFAULT_SIDECAR_SPECS
    _assets_by_stem: dict[str, Asset] = field(default_factory=dict)
    _assets_by_name: dict[str, Asset] = field(default_factory=dict)
    _pending_by_stem: dict[str, list[SidecarRecord]] = field(default_factory=dict)

    def register_asset(self, asset: Asset) -> list[ResolvedSidecar]:
        name, stem = _asset_name_and_stem(asset)
        if not name and not stem:
            return []
        if name:
            self._assets_by_name[name] = asset
        if stem:
            self._assets_by_stem[stem] = asset
        matches: list[ResolvedSidecar] = []
        for key in _candidate_match_keys(name=name, stem=stem):
            pending = self._pending_by_stem.pop(key, [])
            matches.extend(ResolvedSidecar(sidecar=item, asset=asset) for item in pending)
        return matches

    def ingest_sidecar(self, path: str, payload: Any) -> ResolvedSidecar | None:
        sidecar = self._build_sidecar(path, payload)
        if sidecar is None:
            return None
        asset = self._assets_by_name.get(sidecar.target_stem) or self._assets_by_stem.get(
            sidecar.target_stem
        )
        if asset is not None:
            return ResolvedSidecar(sidecar=sidecar, asset=asset)
        self._pending_by_stem.setdefault(sidecar.target_stem, []).append(sidecar)
        return None

    def unresolved(self) -> list[SidecarRecord]:
        items: list[SidecarRecord] = []
        for pending in self._pending_by_stem.values():
            items.extend(pending)
        return items

    def _build_sidecar(self, path: str, payload: Any) -> SidecarRecord | None:
        p = Path(path)
        filename = p.name
        for spec in self.specs:
            if not filename.endswith(spec.suffix):
                continue
            target_stem = filename[: -len(spec.suffix)]
            if not target_stem:
                return None
            return SidecarRecord(
                path=str(p),
                spec=spec,
                payload=payload,
                target_stem=target_stem,
            )
        return None


@dataclass
class SidecarSupport:
    enabled: bool
    resolver: SidecarResolver | None = None

    @classmethod
    def create(cls, *, enabled: bool) -> "SidecarSupport":
        return cls(enabled=enabled, resolver=SidecarResolver() if enabled else None)

    def is_candidate(self, path_or_name: str) -> bool:
        if not self.enabled:
            return False
        return looks_like_sidecar(path_or_name)

    def consume_candidate(
        self,
        *,
        path_or_name: str,
        actor: Actor,
        text: str | None = None,
    ) -> tuple[bool, AssetScanResult | None]:
        if not self.enabled:
            return False, None
        if not looks_like_sidecar(path_or_name):
            return False, None
        payload = parse_sidecar_payload(path_or_name, text=text)
        if payload is None:
            return True, None
        if self.resolver is None:
            return True, None
        resolved = self.resolver.ingest_sidecar(path_or_name, payload)
        if resolved is None:
            return True, None
        result = AssetScanResult(asset=resolved.asset, actor=actor)
        apply_sidecar_payload(result=result, payload=resolved.sidecar.payload)
        return True, result

    def apply_deferred(self, *, asset: Asset, result: AssetScanResult) -> None:
        if not self.enabled or self.resolver is None:
            return
        for deferred in self.resolver.register_asset(asset):
            apply_sidecar_payload(result=result, payload=deferred.sidecar.payload)

    def log_unresolved(self, *, source_name: str, actor_id: int | None) -> None:
        if not self.enabled or self.resolver is None:
            return
        unresolved = self.resolver.unresolved()
        if unresolved:
            logger.warning(
                "{source_name} source {actor_id}: {count} sidecars could not be matched to any asset",
                source_name=source_name,
                actor_id=actor_id,
                count=len(unresolved),
            )


def looks_like_sidecar(path_or_name: str) -> bool:
    name = Path(path_or_name).name
    return any(name.endswith(suffix) for suffix in _SIDECAR_SUFFIXES)


def parse_sidecar_payload(path_or_name: str, *, text: str | None = None) -> dict[str, Any] | None:
    path_str = str(path_or_name)
    if not looks_like_sidecar(path_str):
        return None

    content = text
    if content is None:
        try:
            content = Path(path_str).read_text(encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to read sidecar file {path}: {err}", path=path_str, err=exc)
            return None

    if path_str.endswith(".truth.md"):
        return {"truth_text": content}
    if path_str.endswith(".summary.md"):
        return {"summary_text": content}
    if path_str.endswith(".queries.yml") or path_str.endswith(".queries.yaml"):
        parsed = _parse_yaml_or_json(content)
        if parsed is None:
            return None
        return {"queries": parsed}
    return None


def apply_sidecar_payload(*, result: AssetScanResult, payload: dict[str, Any]) -> None:
    truth_text = payload.get("truth_text")
    if isinstance(truth_text, str) and truth_text.strip():
        result.set_metadata(EVAL_TRUTH_TEXT, truth_text)

    summary_text = payload.get("summary_text")
    if isinstance(summary_text, str) and summary_text.strip():
        result.set_metadata(DOC_SUMMARY, summary_text)

    queries = payload.get("queries")
    if queries is not None:
        result.set_metadata(EVAL_QUERIES, queries)


def _asset_name_and_stem(asset: Asset) -> tuple[str | None, str | None]:
    canonical_uri = (asset.canonical_uri or "").strip()
    if canonical_uri:
        parsed = urlparse(canonical_uri)
        if parsed.scheme == "file":
            name = Path(unquote(parsed.path or "")).name or None
        else:
            name = Path(canonical_uri).name or None
        if not name:
            return None, None
        return name, Path(name).stem or name
    external_id = (asset.external_id or "").strip()
    if not external_id:
        return None, None
    name = Path(external_id).name or external_id
    return name, Path(name).stem or name


def _candidate_match_keys(*, name: str | None, stem: str | None) -> list[str]:
    keys: list[str] = []
    if name:
        keys.append(name)
    if stem and stem not in keys:
        keys.append(stem)
    return keys


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
        logger.warning("Failed to parse sidecar queries payload as YAML/JSON")
        return None
