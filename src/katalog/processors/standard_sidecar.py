from __future__ import annotations

from typing import FrozenSet

from katalog.constants.metadata import (
    DATA_FILE_READER,
    DOC_SUMMARY,
    EVAL_QUERIES,
    EVAL_TRUTH_TEXT,
    FILE_NAME,
    FILE_PATH,
    MetadataKey,
    REL_LINK_TO,
    SIDECAR_TARGET_NAME,
    SIDECAR_TYPE,
)
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult
from katalog.sources.sidecars import detect_sidecar, parse_sidecar_payload


class StandardSidecarProcessor(Processor):
    plugin_id = "katalog.processors.standard_sidecar.StandardSidecarProcessor"
    title = "Standard sidecar decoder"
    description = "Decode .truth.md/.summary.md/.queries.yml sidecars into metadata."
    execution_mode = "io"

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return frozenset({FILE_NAME, FILE_PATH, DATA_FILE_READER})

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return frozenset(
            {
                SIDECAR_TYPE,
                SIDECAR_TARGET_NAME,
                EVAL_TRUTH_TEXT,
                DOC_SUMMARY,
                EVAL_QUERIES,
                REL_LINK_TO,
            }
        )

    def should_run(self, changes: MetadataChanges) -> bool:
        path_or_name = _path_or_name(changes)
        descriptor = detect_sidecar(path_or_name or "")
        if descriptor is None:
            return False
        changed = changes.changed_keys()
        if FILE_NAME in changed or FILE_PATH in changed or DATA_FILE_READER in changed:
            return True
        current = changes.current()
        if SIDECAR_TYPE not in current or SIDECAR_TARGET_NAME not in current:
            return True
        return False

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        asset = changes.asset
        if asset is None:
            return ProcessorResult(status=OpStatus.ERROR, message="MetadataChanges.asset is missing")
        path_or_name = _path_or_name(changes)
        if not path_or_name:
            return ProcessorResult(status=OpStatus.SKIPPED, message="No path/name available")
        descriptor = detect_sidecar(path_or_name)
        if descriptor is None:
            return ProcessorResult(status=OpStatus.SKIPPED, message="Not a sidecar asset")

        reader = await asset.get_data_reader(DATA_FILE_READER, changes)
        if reader is None:
            return ProcessorResult(status=OpStatus.SKIPPED, message="No data reader for sidecar")
        raw = await reader.read()
        text = raw.decode("utf-8", errors="replace")
        payload = parse_sidecar_payload(path_or_name, text=text)
        if payload is None:
            return ProcessorResult(status=OpStatus.SKIPPED, message="Unsupported sidecar payload")

        metadata = [
            make_metadata(SIDECAR_TYPE, descriptor.kind, actor_id=self.actor.id),
            make_metadata(SIDECAR_TARGET_NAME, descriptor.target_name, actor_id=self.actor.id),
            # Always clear old link before link analyzer refreshes it.
            make_metadata(REL_LINK_TO, None, actor_id=self.actor.id, removed=True),
        ]
        truth_text = payload.get("truth_text")
        if isinstance(truth_text, str) and truth_text.strip():
            metadata.append(make_metadata(EVAL_TRUTH_TEXT, truth_text, actor_id=self.actor.id))
        summary_text = payload.get("summary_text")
        if isinstance(summary_text, str) and summary_text.strip():
            metadata.append(make_metadata(DOC_SUMMARY, summary_text, actor_id=self.actor.id))
        queries = payload.get("queries")
        if queries is not None:
            metadata.append(make_metadata(EVAL_QUERIES, queries, actor_id=self.actor.id))

        return ProcessorResult(status=OpStatus.COMPLETED, metadata=metadata)


def _path_or_name(changes: MetadataChanges) -> str | None:
    current = changes.current()
    for key in (FILE_PATH, FILE_NAME):
        values = current.get(key, [])
        if not values:
            continue
        value = values[0].value
        if isinstance(value, str) and value.strip():
            return value.strip()
    asset = changes.asset
    if asset is None:
        return None
    candidate = (asset.external_id or "").strip()
    return candidate or None
