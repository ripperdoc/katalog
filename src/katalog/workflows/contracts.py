from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Union

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
