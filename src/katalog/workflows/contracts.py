from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Union

from katalog.models import Asset, Metadata, MetadataChanges, OpStatus


@dataclass(frozen=True)
class WorkflowSourceActorsInput:
    """Select assets by scanning one or more source actors."""

    kind: Literal["source_actors"] = "source_actors"
    actor_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class WorkflowAllAssetsInput:
    """Select all assets in the workspace."""

    kind: Literal["all_assets"] = "all_assets"


@dataclass(frozen=True)
class WorkflowCollectionInput:
    """Select assets from one collection."""

    kind: Literal["collection"] = "collection"
    collection_id: int = 0


@dataclass(frozen=True)
class WorkflowAssetIdsInput:
    """Select an explicit set of asset ids."""

    kind: Literal["asset_ids"] = "asset_ids"
    asset_ids: list[int] = field(default_factory=list)


WorkflowInputSpec = Union[
    WorkflowSourceActorsInput,
    WorkflowAllAssetsInput,
    WorkflowCollectionInput,
    WorkflowAssetIdsInput,
]


def parse_workflow_input_payload(
    payload: Mapping[str, Any] | WorkflowInputSpec | None,
) -> WorkflowInputSpec | None:
    """Parse and validate one workflow input selector payload."""
    if payload is None or isinstance(
        payload,
        (
            WorkflowSourceActorsInput,
            WorkflowAllAssetsInput,
            WorkflowCollectionInput,
            WorkflowAssetIdsInput,
        ),
    ):
        return payload

    kind = payload.get("kind")
    if kind == "source_actors":
        raw_actor_ids = payload.get("actor_ids") or []
        if not isinstance(raw_actor_ids, list):
            raise ValueError("input.actor_ids must be a list of integers")
        return WorkflowSourceActorsInput(actor_ids=[int(value) for value in raw_actor_ids])
    if kind == "all_assets":
        return WorkflowAllAssetsInput()
    if kind == "collection":
        if "collection_id" not in payload:
            raise ValueError("input.collection_id is required for collection input")
        return WorkflowCollectionInput(collection_id=int(payload["collection_id"]))
    if kind == "asset_ids":
        raw_asset_ids = payload.get("asset_ids") or []
        if not isinstance(raw_asset_ids, list):
            raise ValueError("input.asset_ids must be a list of integers")
        return WorkflowAssetIdsInput(asset_ids=[int(value) for value in raw_asset_ids])
    raise ValueError(
        "input.kind must be one of: source_actors, all_assets, collection, asset_ids"
    )


def workflow_input_to_payload(workflow_input: WorkflowInputSpec) -> dict[str, Any]:
    """Serialize workflow input selector to a JSON-compatible payload."""
    if isinstance(workflow_input, WorkflowSourceActorsInput):
        return {"kind": "source_actors", "actor_ids": list(workflow_input.actor_ids)}
    if isinstance(workflow_input, WorkflowCollectionInput):
        return {"kind": "collection", "collection_id": int(workflow_input.collection_id)}
    if isinstance(workflow_input, WorkflowAssetIdsInput):
        return {"kind": "asset_ids", "asset_ids": list(workflow_input.asset_ids)}
    return {"kind": "all_assets"}


@dataclass(frozen=True)
class SourceAssetPayload:
    """One source-emitted asset plus metadata before DB hydration."""

    asset: Asset
    actor_id: int
    metadata: list[Metadata] = field(default_factory=list)


@dataclass(frozen=True)
class RecursionSeed:
    """A queued recursive scan unit for a source actor and depth."""

    actor_id: int
    changes: MetadataChanges
    depth: int


@dataclass(frozen=True)
class SourceBatch:
    """Source-emitted batch used by the workflow loading stage."""

    items: list[SourceAssetPayload]
    ignored: int = 0
    status: OpStatus = OpStatus.IN_PROGRESS
    recursion_seeds: list[RecursionSeed] = field(default_factory=list)


@dataclass(frozen=True)
class ProcessorBatch:
    """Logical processor input batch wrapper."""

    changes: list[MetadataChanges]


@dataclass(frozen=True)
class ProcessorBatchResult:
    """Per-asset processor outputs for one stage."""

    results: list[list[Metadata]] = field(default_factory=list)
    status: OpStatus = OpStatus.COMPLETED


@dataclass(frozen=True)
class StageBatchEnvelope:
    """Optional envelope when stages need richer cross-stage payloads."""

    batch_id: int
    source_batch: SourceBatch | None = None
    processor_batch: ProcessorBatch | None = None
