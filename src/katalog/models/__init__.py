"""Backwards-compatible exports for the split model modules."""

from katalog.constants.metadata import MetadataKey, MetadataScalar, MetadataType

from .assets import (
    Asset,
    AssetCollection,
    CollectionItem,
    CollectionRefreshMode,
    FileAccessor,
)
from .core import (
    Actor,
    ActorType,
    Changeset,
    ChangesetStats,
    DEFAULT_TASK_CONCURRENCY,
    OpStatus,
    drain_tasks,
)
from .metadata import (
    Metadata,
    MetadataChangeSet,
    MetadataRegistry,
    make_metadata,
)

__all__ = [
    "Actor",
    "ActorType",
    "Asset",
    "AssetCollection",
    "Changeset",
    "ChangesetStats",
    "CollectionItem",
    "CollectionRefreshMode",
    "DEFAULT_TASK_CONCURRENCY",
    "FileAccessor",
    "Metadata",
    "MetadataChangeSet",
    "MetadataKey",
    "MetadataRegistry",
    "MetadataScalar",
    "MetadataType",
    "OpStatus",
    "drain_tasks",
    "make_metadata",
]
