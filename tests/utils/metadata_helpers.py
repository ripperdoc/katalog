from dataclasses import replace
from typing import Any

import pytest

from katalog.constants.metadata import (
    FILE_PATH,
    MetadataDef,
    MetadataKey,
    MetadataType,
    METADATA_REGISTRY_BY_ID,
    METADATA_REGISTRY,
    get_metadata_id,
)
from katalog.models import Metadata


def mem_md(
    *,
    key: MetadataKey,
    value: Any,
    changeset_id: int,
    actor_id: int = 1,
    removed: bool = False,
    registry_id: int | None = None,
) -> Metadata:
    registry_id = registry_id or get_metadata_id(key)
    entry = Metadata(
        metadata_key_id=registry_id,
        value_type=MetadataType.STRING,
        changeset_id=changeset_id,
        actor_id=actor_id,
        removed=removed,
    )
    entry.metadata_key_id = registry_id
    entry.changeset_id = changeset_id
    entry.actor_id = actor_id
    entry.value_text = str(value)
    return entry


@pytest.fixture
def registry_stub():
    saved_by_id = METADATA_REGISTRY_BY_ID.copy()
    saved_def = METADATA_REGISTRY[FILE_PATH]

    patched = replace(saved_def, registry_id=1)
    METADATA_REGISTRY[FILE_PATH] = patched
    METADATA_REGISTRY_BY_ID.clear()
    METADATA_REGISTRY_BY_ID[1] = patched
    yield
    METADATA_REGISTRY[FILE_PATH] = saved_def
    METADATA_REGISTRY_BY_ID.clear()
    METADATA_REGISTRY_BY_ID.update(saved_by_id)
