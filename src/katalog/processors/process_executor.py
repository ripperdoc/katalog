from __future__ import annotations

import asyncio
import logging
from typing import Any

from katalog.processors.serialization import (
    normalize_metadata_changes_payload,
    seed_registry,
)
from katalog.models import Actor, ActorType, MetadataChanges, OpStatus
from katalog.processors.base import ProcessorResult
from katalog.plugins.registry import get_actor_instance


_REGISTRY_READY = False


async def _run_processor(
    *,
    actor: Actor,
    changes: MetadataChanges,
) -> ProcessorResult:
    processor = await get_actor_instance(actor)
    return await processor.run(changes)


def run_processor_in_process(
    actor_payload: dict[str, Any],
    changes_payload: dict[str, Any],
    registry_payload: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        global _REGISTRY_READY
        if not _REGISTRY_READY:
            if registry_payload is not None:
                seed_registry(registry_payload)
            _REGISTRY_READY = True
        actor_type = actor_payload.get("type")
        if isinstance(actor_type, str):
            try:
                actor_payload = dict(actor_payload)
                actor_payload["type"] = ActorType[actor_type]
            except KeyError:
                pass
        actor = Actor.model_validate(actor_payload)
        normalized_changes = normalize_metadata_changes_payload(changes_payload)
        changes = MetadataChanges.model_validate(normalized_changes)
        result = asyncio.run(_run_processor(actor=actor, changes=changes))
        return result.model_dump(mode="json")
    except Exception as exc:  # noqa: BLE001
        asset_payload = changes_payload.get("asset") if isinstance(changes_payload, dict) else None
        asset_id = asset_payload.get("id") if isinstance(asset_payload, dict) else None
        msg = f"Processor failed for asset {asset_id}: {exc}"
        logging.exception(msg)
        return ProcessorResult(status=OpStatus.ERROR, message=msg).model_dump(mode="json")
