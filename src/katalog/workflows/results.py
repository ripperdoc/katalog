from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_serializer

from katalog.models import Changeset, OpStatus


class WorkflowChangesetResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    changeset_id: int
    status: OpStatus
    error_message: str | None = None

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)

    @classmethod
    def from_changeset(cls, changeset: Changeset) -> "WorkflowChangesetResult":
        error_message = None
        if isinstance(changeset.data, dict):
            raw_error = changeset.data.get("error_message")
            if isinstance(raw_error, str) and raw_error.strip():
                error_message = raw_error.strip()
        return cls(
            changeset_id=int(changeset.id),
            status=changeset.status,
            error_message=error_message,
        )


class WorkflowRunResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    workflow_file: str
    actors: int
    sources_run: int
    processors_run: int
    analyzers_run: int
    source_changesets: list[int]
    processor_changeset: int | None
    analyzer_changesets: list[int]
    last_changeset_id: int | None
    status: OpStatus
    successful: bool
    changeset_results: list[WorkflowChangesetResult]
    error_message: str | None = None

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)

    @classmethod
    def build(
        cls,
        *,
        workflow_file: str,
        actors: int,
        processors_run: int,
        source_results: list[WorkflowChangesetResult],
        processor_result: WorkflowChangesetResult | None,
        analyzer_results: list[WorkflowChangesetResult],
    ) -> "WorkflowRunResult":
        changeset_results = [
            *source_results,
            *([processor_result] if processor_result is not None else []),
            *analyzer_results,
        ]
        statuses = [entry.status for entry in changeset_results]
        if any(status == OpStatus.ERROR for status in statuses):
            status = OpStatus.ERROR
        elif any(status == OpStatus.CANCELED for status in statuses):
            status = OpStatus.CANCELED
        elif any(status == OpStatus.PARTIAL for status in statuses):
            status = OpStatus.PARTIAL
        elif any(status == OpStatus.SKIPPED for status in statuses):
            status = OpStatus.PARTIAL
        else:
            status = OpStatus.COMPLETED

        source_changesets = [entry.changeset_id for entry in source_results]
        processor_changeset = (
            processor_result.changeset_id if processor_result is not None else None
        )
        analyzer_changesets = [entry.changeset_id for entry in analyzer_results]

        error_message = next(
            (
                entry.error_message
                for entry in changeset_results
                if entry.error_message
            ),
            None,
        )

        return cls(
            workflow_file=workflow_file,
            actors=actors,
            sources_run=len(source_changesets),
            processors_run=processors_run,
            analyzers_run=len(analyzer_changesets),
            source_changesets=source_changesets,
            processor_changeset=processor_changeset,
            analyzer_changesets=analyzer_changesets,
            last_changeset_id=(
                analyzer_changesets[-1]
                if analyzer_changesets
                else (
                    processor_changeset
                    if processor_changeset is not None
                    else (source_changesets[-1] if source_changesets else None)
                )
            ),
            status=status,
            successful=(status == OpStatus.COMPLETED),
            changeset_results=changeset_results,
            error_message=error_message,
        )
