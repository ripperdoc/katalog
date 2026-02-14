from __future__ import annotations

from katalog.models import Asset
from katalog.sources.sidecars import SidecarResolver


def _asset(uri: str) -> Asset:
    return Asset(
        namespace="fs:1",
        external_id=f"path:{uri}",
        canonical_uri=uri,
        actor_id=1,
    )


def test_sidecar_resolver_matches_after_asset_registration() -> None:
    resolver = SidecarResolver()
    sidecar_path = "/tmp/abc123#doc.pdf.truth.md"
    payload = {"truth_text": "hello"}

    resolved = resolver.ingest_sidecar(sidecar_path, payload)
    assert resolved is None

    matches = resolver.register_asset(_asset("file:///tmp/abc123%23doc.pdf"))
    assert len(matches) == 1
    assert matches[0].sidecar.path == sidecar_path
    assert matches[0].sidecar.payload == payload
    assert matches[0].asset.canonical_uri == "file:///tmp/abc123%23doc.pdf"
    assert resolver.unresolved() == []


def test_sidecar_resolver_matches_immediately_if_asset_known() -> None:
    resolver = SidecarResolver()
    asset = _asset("file:///tmp/a.pdf")
    resolver.register_asset(asset)

    resolved = resolver.ingest_sidecar("/tmp/a.pdf.summary.md", {"summary_text": "s"})
    assert resolved is not None
    assert resolved.asset.canonical_uri == asset.canonical_uri
    assert resolved.sidecar.target_stem == "a.pdf"
