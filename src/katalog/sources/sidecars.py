from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from katalog.models import Asset


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
