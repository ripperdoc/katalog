from abc import ABC, abstractmethod
from typing import FrozenSet, cast

from pydantic import BaseModel, Field, field_serializer

from katalog.models import (
    Asset,
    MetadataKey,
    Metadata,
    OpStatus,
    Actor,
    MetadataChanges,
)
from katalog.constants.metadata import DATA_KEY, HASH_MD5, TIME_MODIFIED
from katalog.plugins.base import PluginBase

from katalog.plugins.registry import get_actor_instance


class ProcessorResult(BaseModel):
    metadata: list[Metadata] = Field(default_factory=list)
    assets: list[Asset] = Field(default_factory=list)
    status: OpStatus = OpStatus.COMPLETED
    message: str | None = None

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)


class Processor(PluginBase, ABC):
    """
    Defines the interface for a metadata processor.
    """

    @property
    @abstractmethod
    def dependencies(self) -> FrozenSet[MetadataKey]:
        """Return dependencies for this processor instance."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def outputs(self) -> FrozenSet[MetadataKey]:
        """Return outputs for this processor instance."""
        raise NotImplementedError()

    @abstractmethod
    def should_run(
        self,
        asset: Asset,
        changes: MetadataChanges,
    ) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    @abstractmethod
    async def run(
        self,
        asset: Asset,
        changes: MetadataChanges,
    ) -> ProcessorResult:
        """Run the processor logic and return a result class with changes to persist."""
        raise NotImplementedError()


def file_data_changed(
    self,
    asset: Asset,
    changes: MetadataChanges,
    allow_weak_check: bool = True,
) -> bool:
    """Helper to determine if data or relevant fields have changed. If allow_weak_check is True, also assume data has changed if TIME_MODIFIED has changed."""
    changed_keys = changes.changed_keys()
    return (
        DATA_KEY in changed_keys
        or HASH_MD5 in changed_keys
        or (allow_weak_check and TIME_MODIFIED in changed_keys)
    )


file_data_change_dependencies = frozenset({DATA_KEY, HASH_MD5, TIME_MODIFIED})


async def make_processor_instance(processor_record: Actor) -> Processor:
    return cast(Processor, await get_actor_instance(processor_record))
