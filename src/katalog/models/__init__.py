"""Backwards-compatible exports for the split model modules."""

from katalog.constants.metadata import MetadataKey, MetadataScalar, MetadataType

from .assets import (
    Asset,
    AssetCollection,
    CollectionRefreshMode,
    DataReader,
)
from .core import (
    Actor,
    ActorType,
    ChangesetActor,
    Changeset,
    ChangesetStats,
    DEFAULT_TASK_CONCURRENCY,
    OpStatus,
    drain_tasks,
)
from .metadata import (
    Metadata,
    MetadataChanges,
    MetadataRegistry,
    make_metadata,
)

__all__ = [
    "Actor",
    "ActorType",
    "Asset",
    "AssetCollection",
    "ChangesetActor",
    "Changeset",
    "ChangesetStats",
    "CollectionRefreshMode",
    "DEFAULT_TASK_CONCURRENCY",
    "DataReader",
    "Metadata",
    "MetadataChanges",
    "MetadataKey",
    "MetadataRegistry",
    "MetadataScalar",
    "MetadataType",
    "OpStatus",
    "drain_tasks",
    "make_metadata",
]
