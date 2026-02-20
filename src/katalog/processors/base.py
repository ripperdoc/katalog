from abc import ABC, abstractmethod
from typing import Collection, FrozenSet, cast

from pydantic import BaseModel, Field, field_serializer

from katalog.models import (
    Asset,
    MetadataKey,
    Metadata,
    MetadataScalar,
    OpStatus,
    Actor,
    MetadataChanges,
    make_metadata,
)
from katalog.constants.metadata import DATA_KEY, HASH_MD5, TIME_MODIFIED
from katalog.plugins.base import PluginBase

from katalog.plugins.registry import get_actor_instance


class ProcessorResult(BaseModel):
    actor_id: int | None = None
    metadata: list[Metadata] = Field(default_factory=list)
    assets: list[Asset] = Field(default_factory=list)
    status: OpStatus = OpStatus.COMPLETED
    message: str | None = None

    def set_metadata(self, metadata_key: MetadataKey, value: MetadataScalar) -> None:
        """Append a metadata value produced by this processor."""
        if self.actor_id is None:
            raise ValueError("ProcessorResult.actor_id is required to set metadata")
        self.metadata.append(make_metadata(metadata_key, value, self.actor_id))

    def set_metadata_list(
        self,
        metadata_key: MetadataKey,
        values: Collection[MetadataScalar],
    ) -> None:
        """Append multiple values for the same metadata key."""
        for value in values:
            self.set_metadata(metadata_key, value)

    @field_serializer("status")
    def _serialize_status(self, value: OpStatus) -> str:
        return value.value if isinstance(value, OpStatus) else str(value)


class Processor(PluginBase, ABC):
    """
    Defines the interface for a metadata processor.
    """
    execution_mode: str = "io"

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

    async def is_ready(self) -> tuple[bool, str | None]:
        """Return whether the processor can execute in the current environment.

        This check is run once per processor per run, not per asset.
        """
        return True, None

    @abstractmethod
    def should_run(
        self,
        changes: MetadataChanges,
    ) -> bool:
        """Return True if the processor needs to run based on record and the metadata fields that have changed in it."""
        raise NotImplementedError()

    @abstractmethod
    async def run(
        self,
        changes: MetadataChanges,
    ) -> ProcessorResult:
        """Run the processor logic and return a result class with changes to persist."""
        raise NotImplementedError()


def file_data_changed(
    self,
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
