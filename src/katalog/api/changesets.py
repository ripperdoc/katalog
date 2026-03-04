import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field

from katalog.models import Changeset, Metadata, MetadataChanges, OpStatus
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.utils.changeset_events import sse_event

from katalog.editors.user_editor import ensure_user_editor
from katalog.api.helpers import ApiError
from katalog.api.schemas import ChangesetChangesResponse, ChangesetDiffResponse
from katalog.runtime.state import get_event_manager, get_running_changesets


class ChangesetUpdate(BaseModel):
    """Payload for updating mutable changeset fields."""
    message: str | None = Field(default=None)


async def create_changeset() -> Changeset:
    """Start a manual-edit changeset for the user editor actor."""
    actor = await ensure_user_editor()
    db = get_changeset_repo()
    try:
        changeset = await db.begin(
            actors=[actor],
            message="Manual edit",
            status=OpStatus.IN_PROGRESS,
            data={"manual": True},
        )
    except ValueError as exc:
        raise ApiError(status_code=409, detail=str(exc))
    return changeset


async def finish_changeset(changeset_id: int) -> Changeset:
    """Finalize an in-progress changeset as completed."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    if changeset.status != OpStatus.IN_PROGRESS:
        raise ApiError(status_code=409, detail="Changeset is not in progress")
    await changeset.finalize(status=OpStatus.COMPLETED)
    latest = await db.get(id=changeset_id)
    await db.load_actor_ids(latest)
    return latest


async def list_changesets() -> list[Changeset]:
    """List all changesets with actor ids loaded."""
    db = get_changeset_repo()
    changesets = await db.list_rows(order_by="id DESC")
    for changeset in changesets:
        await db.load_actor_ids(changeset)
    return changesets


async def get_changeset(
    changeset_id: int,
) -> tuple[Changeset, list[dict[str, object]], bool]:
    """Return a changeset, buffered events, and running state."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    await db.load_actor_ids(changeset)

    return (
        changeset,
        get_event_manager().get_buffer(changeset_id),
        changeset.status == OpStatus.IN_PROGRESS,
    )


async def delete_changeset(changeset_id: int) -> dict[str, int | str]:
    """Undo a changeset by deleting it (cascade removes related rows)."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    await db.delete(changeset)
    return {"status": "deleted", "changeset_id": changeset_id}


async def list_changeset_changes(
    changeset_id: int,
    *,
    offset: int = 0,
    limit: int = 200,
    from_changeset_id: int | None = None,
    to_changeset_id: int | None = None,
) -> ChangesetChangesResponse:
    """List raw metadata changes for one changeset or a changeset range."""
    db = get_changeset_repo()
    first_changeset_id, last_changeset_id = await _resolve_changeset_range(
        db=db,
        changeset_id=changeset_id,
        from_changeset_id=from_changeset_id,
        to_changeset_id=to_changeset_id,
    )

    try:
        if first_changeset_id == last_changeset_id:
            return await db.list_changeset_metadata_changes(
                first_changeset_id, offset=offset, limit=limit, include_total=True
            )
        return await db.list_metadata_changes_in_range(
            from_changeset_id=first_changeset_id,
            to_changeset_id=last_changeset_id,
            offset=offset,
            limit=limit,
            include_total=True,
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))


async def _resolve_changeset_range(
    *,
    db,
    changeset_id: int,
    from_changeset_id: int | None,
    to_changeset_id: int | None,
) -> tuple[int, int]:
    """Resolve and validate an inclusive changeset id range."""
    first_changeset_id = (
        int(from_changeset_id)
        if from_changeset_id is not None
        else int(changeset_id)
    )
    last_changeset_id = (
        int(to_changeset_id)
        if to_changeset_id is not None
        else int(changeset_id)
    )
    if first_changeset_id > last_changeset_id:
        raise ApiError(
            status_code=400, detail="from_changeset_id must be <= to_changeset_id"
        )
    first_changeset = await db.get_or_none(id=first_changeset_id)
    if first_changeset is None:
        raise ApiError(status_code=404, detail="from_changeset_id not found")
    last_changeset = await db.get_or_none(id=last_changeset_id)
    if last_changeset is None:
        raise ApiError(status_code=404, detail="to_changeset_id not found")
    return first_changeset_id, last_changeset_id


def _sort_key_for_value(value: Any) -> str:
    """Build a stable sort key for metadata fingerprints."""
    if isinstance(value, dict):
        items = sorted((str(k), _sort_key_for_value(v)) for k, v in value.items())
        return str(items)
    if isinstance(value, list):
        return str([_sort_key_for_value(item) for item in value])
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:
            return str(value)
    return str(value)


def _metadata_value_to_json(value: Any) -> Any:
    """Convert metadata values to JSON-serializable output."""
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value


def _pair_before_after_rows(
    *,
    asset_id: int,
    metadata_key: str,
    metadata_key_id: int,
    actor_id: int,
    before_entries: list[Metadata],
    after_entries: list[Metadata],
) -> list[dict[str, Any]]:
    """Pair metadata entries into changed, removed, and added diff rows."""
    before_by_fp = {
        entry.fingerprint(): entry
        for entry in before_entries
        if entry.fingerprint() is not None
    }
    after_by_fp = {
        entry.fingerprint(): entry
        for entry in after_entries
        if entry.fingerprint() is not None
    }
    unchanged = set(before_by_fp.keys()) & set(after_by_fp.keys())
    before_only = [entry for fp, entry in before_by_fp.items() if fp not in unchanged]
    after_only = [entry for fp, entry in after_by_fp.items() if fp not in unchanged]

    before_only.sort(key=lambda entry: _sort_key_for_value(entry.fingerprint()))
    after_only.sort(key=lambda entry: _sort_key_for_value(entry.fingerprint()))

    rows: list[dict[str, Any]] = []
    pair_count = min(len(before_only), len(after_only))
    for idx in range(pair_count):
        before_entry = before_only[idx]
        after_entry = after_only[idx]
        rows.append(
            {
                "id": f"{asset_id}:{metadata_key_id}:{actor_id}:changed:{idx}",
                "asset_id": asset_id,
                "metadata_key": metadata_key,
                "metadata_key_id": metadata_key_id,
                "actor_id": actor_id,
                "change_type": "changed",
                "before": _metadata_value_to_json(before_entry.value),
                "after": _metadata_value_to_json(after_entry.value),
            }
        )

    for idx, entry in enumerate(before_only[pair_count:], start=pair_count):
        rows.append(
            {
                "id": f"{asset_id}:{metadata_key_id}:{actor_id}:removed:{idx}",
                "asset_id": asset_id,
                "metadata_key": metadata_key,
                "metadata_key_id": metadata_key_id,
                "actor_id": actor_id,
                "change_type": "removed",
                "before": _metadata_value_to_json(entry.value),
                "after": None,
            }
        )

    for idx, entry in enumerate(after_only[pair_count:], start=pair_count):
        rows.append(
            {
                "id": f"{asset_id}:{metadata_key_id}:{actor_id}:added:{idx}",
                "asset_id": asset_id,
                "metadata_key": metadata_key,
                "metadata_key_id": metadata_key_id,
                "actor_id": actor_id,
                "change_type": "added",
                "before": None,
                "after": _metadata_value_to_json(entry.value),
            }
        )

    return rows


def _unsupported_diff_query_warnings(
    *,
    search: str | None,
    sort: list[str] | None,
    filters: list[str] | None,
) -> list[str]:
    """Return warnings for unsupported diff query options."""
    has_unsupported = bool((search and search.strip()) or sort or filters)
    if not has_unsupported:
        return []
    return ["Ignoring search/sort/filters in changeset diff view."]


def _build_asset_diff_rows(
    *,
    asset_id: int,
    metadata_rows: list[Metadata],
    from_changeset_id: int,
    to_changeset_id: int,
) -> list[dict[str, Any]]:
    """Build per-asset metadata diff rows across a changeset range."""
    changes = MetadataChanges(loaded=metadata_rows)
    before_by_actor = changes.state_before_by_actor(from_changeset_id)
    after_by_actor = changes.state_after_by_actor(to_changeset_id)
    actor_ids = sorted(set(before_by_actor.keys()) | set(after_by_actor.keys()))

    rows: list[dict[str, Any]] = []
    for actor_id in actor_ids:
        before_by_key = before_by_actor.get(actor_id, {})
        after_by_key = after_by_actor.get(actor_id, {})
        keys = sorted(
            set(before_by_key.keys()) | set(after_by_key.keys()),
            key=str,
        )
        for key in keys:
            before_entries = before_by_key.get(key, [])
            after_entries = after_by_key.get(key, [])
            if not before_entries and not after_entries:
                continue
            reference_entry = (
                before_entries[0] if before_entries else after_entries[0]
            )
            metadata_key_id = reference_entry.metadata_key_id
            if metadata_key_id is None:
                continue
            key_rows = _pair_before_after_rows(
                asset_id=int(asset_id),
                metadata_key=str(key),
                metadata_key_id=int(metadata_key_id),
                actor_id=int(actor_id),
                before_entries=before_entries,
                after_entries=after_entries,
            )
            rows.extend(key_rows)
    return rows


async def list_changeset_diff(
    changeset_id: int,
    *,
    offset: int = 0,
    limit: int = 200,
    from_changeset_id: int | None = None,
    to_changeset_id: int | None = None,
    search: str | None = None,
    sort: list[str] | None = None,
    filters: list[str] | None = None,
) -> ChangesetDiffResponse:
    """List metadata diff rows for changed assets in a changeset range."""
    started_at = time.perf_counter()
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    first_changeset_id, last_changeset_id = await _resolve_changeset_range(
        db=db,
        changeset_id=changeset_id,
        from_changeset_id=from_changeset_id,
        to_changeset_id=to_changeset_id,
    )
    warnings = _unsupported_diff_query_warnings(
        search=search,
        sort=sort,
        filters=filters,
    )

    assets_started = time.perf_counter()
    try:
        asset_ids, total_assets = await db.list_changed_asset_ids_in_range(
            from_changeset_id=first_changeset_id,
            to_changeset_id=last_changeset_id,
            offset=offset,
            limit=limit,
            include_total=True,
        )
    except ValueError as exc:
        raise ApiError(status_code=400, detail=str(exc))
    duration_assets_ms = int((time.perf_counter() - assets_started) * 1000)

    metadata_started = time.perf_counter()
    metadata_by_asset: dict[int, list[Metadata]] = {}
    if asset_ids:
        md_db = get_metadata_repo()
        metadata_by_asset = await md_db.for_assets(asset_ids, include_removed=True)
    duration_metadata_ms = int((time.perf_counter() - metadata_started) * 1000)

    leaf_rows: list[dict[str, Any]] = []
    for asset_id in asset_ids:
        metadata_rows = metadata_by_asset.get(int(asset_id), [])
        leaf_rows.extend(
            _build_asset_diff_rows(
                asset_id=int(asset_id),
                metadata_rows=metadata_rows,
                from_changeset_id=first_changeset_id,
                to_changeset_id=last_changeset_id,
            )
        )
    duration_ms = int((time.perf_counter() - started_at) * 1000)

    return ChangesetDiffResponse.model_validate(
        {
            "mode": "diff",
            "items": leaf_rows,
            "warnings": warnings,
            "range": {
                "from_changeset_id": first_changeset_id,
                "to_changeset_id": last_changeset_id,
            },
            "stats": {
                "returned": len(leaf_rows),
                "total": total_assets,
                "duration_ms": duration_ms,
                "duration_assets_ms": duration_assets_ms,
                "duration_metadata_ms": duration_metadata_ms,
            },
            "pagination": {"offset": offset, "limit": limit},
        }
    )


async def update_changeset(changeset_id: int, payload: ChangesetUpdate) -> Changeset:
    """Update mutable properties of a changeset."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    if payload.message is not None:
        changeset.message = payload.message
        await db.save(changeset)
    await db.load_actor_ids(changeset)
    return changeset


def _status_event_for(changeset: Changeset) -> dict[str, object]:
    """Build an SSE payload with current changeset status."""
    payload = changeset.model_dump(mode="json")
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "event": "changeset_status",
        "changeset_id": changeset.id,
        "ts": ts,
        "payload": payload,
    }


def _heartbeat_event(changeset_id: int) -> dict[str, object]:
    """Build a keepalive heartbeat SSE payload."""
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "event": "heartbeat",
        "changeset_id": changeset_id,
        "ts": ts,
        "payload": {},
    }


async def stream_changeset_events(changeset_id: int):
    """Stream buffered and live events for a changeset as SSE payloads."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")

    event_manager = get_event_manager()
    running_changesets = get_running_changesets()
    history, queue = event_manager.subscribe(changeset_id)
    run_state = running_changesets.get(changeset_id)
    done_event = (
        run_state.done_event if run_state and run_state.done_event else asyncio.Event()
    )
    if run_state is None and changeset.status != OpStatus.IN_PROGRESS:
        done_event.set()
    poll_task: asyncio.Task | None = None
    if run_state is None and changeset.status == OpStatus.IN_PROGRESS:
        async def _poll_status() -> None:
            while not done_event.is_set():
                await asyncio.sleep(1.0)
                latest = await db.get(id=changeset_id)
                if latest.status != OpStatus.IN_PROGRESS:
                    done_event.set()
                    break

        poll_task = asyncio.create_task(_poll_status())

    async def event_generator():
        try:
            for event in history:
                yield sse_event(event)
            yield sse_event(_status_event_for(changeset))
            while True:
                done_waiter = asyncio.create_task(done_event.wait())
                event_waiter = asyncio.create_task(queue.get())
                done, pending = await asyncio.wait(
                    {done_waiter, event_waiter},
                    timeout=10,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                if not done:
                    yield sse_event(_heartbeat_event(changeset_id))
                    continue
                if event_waiter in done:
                    message = event_waiter.result()
                    yield sse_event(message)
                else:
                    event_waiter.cancel()
                if done_waiter in done:
                    latest = await db.get(id=changeset_id)
                    await db.load_actor_ids(latest)
                    yield sse_event(_status_event_for(latest))
                    break
        finally:
            event_manager.unsubscribe(changeset_id, queue)
            if poll_task is not None:
                poll_task.cancel()

    return event_generator()


async def cancel_changeset(
    changeset_id: int,
) -> tuple[str, Changeset | None]:
    """Request cancellation for a running changeset operation."""
    db = get_changeset_repo()
    changeset = await db.get_or_none(id=changeset_id)
    if changeset is None:
        raise ApiError(status_code=404, detail="Changeset not found")
    running_changesets = get_running_changesets()
    running_changeset = running_changesets.get(changeset_id)
    if running_changeset is None or (
        running_changeset.done_event and running_changeset.done_event.is_set()
    ):
        # Nothing running (maybe after restart). If still in progress, finalize as canceled.
        if changeset.status == OpStatus.IN_PROGRESS:
            await changeset.finalize(status=OpStatus.CANCELED)
            changeset = await db.get(id=changeset_id)
            await db.load_actor_ids(changeset)
        return "cancelled", changeset

    # Signal cancellation and wait briefly
    running_changeset.cancel()
    try:
        await running_changeset.wait_cancelled(timeout=10)
    except asyncio.TimeoutError:
        return "cancellation_requested", None

    latest = await db.get(id=changeset_id)
    await db.load_actor_ids(latest)
    return "cancelled", latest
