from __future__ import annotations

from katalog.sources.sidecars import detect_sidecar, parse_sidecar_payload


def test_detect_sidecar_truth() -> None:
    descriptor = detect_sidecar("/tmp/a.pdf.truth.md")
    assert descriptor is not None
    assert descriptor.kind == "truth_markdown"
    assert descriptor.target_name == "a.pdf"


def test_parse_queries_yaml_payload() -> None:
    payload = parse_sidecar_payload(
        "doc.queries.yml",
        text="- query: hello\n  quote: world\n",
    )
    assert payload is not None
    assert isinstance(payload.get("queries"), list)
    assert payload["queries"][0]["query"] == "hello"
