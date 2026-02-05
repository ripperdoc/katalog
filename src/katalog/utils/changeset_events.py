import asyncio
import json
from collections import deque
from datetime import datetime, timezone
from typing import Any

from loguru import logger


def sse_event(event: dict[str, Any]) -> str:
    event_name = event.get("event", "message")
    data = json.dumps(event, default=str)
    return f"event: {event_name}\ndata: {data}\n\n"


class ChangesetEventManager:
    """Keep a small in-memory buffer of changeset logs and stream new ones."""

    def __init__(self, *, max_events: int = 200):
        self.max_events = max_events
        self.buffers: dict[int, deque[dict[str, Any]]] = {}
        self.listeners: dict[int, set[asyncio.Queue[dict[str, Any]]]] = {}
        self.loop: asyncio.AbstractEventLoop | None = None
        self._sink_added = False

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def ensure_sink(self) -> None:
        if self._sink_added:
            return
        logger.add(
            self._handle_message,
            filter=lambda record: "changeset_id" in record["extra"],
            backtrace=False,
            diagnose=False,
        )
        self._sink_added = True

    def _handle_message(self, message) -> None:
        extra = message.record["extra"]
        changeset_id = extra.get("changeset_id")
        if changeset_id is None:
            return
        if self.loop is None:
            return
        ts = message.record["time"].astimezone(timezone.utc).isoformat()
        events: list[dict[str, Any]] = []

        progress = extra.get("changeset_progress")
        if isinstance(progress, dict):
            events.append(
                self.make_event(
                    int(changeset_id),
                    "changeset_progress",
                    payload=progress,
                    ts=ts,
                )
            )

        status = extra.get("changeset_status")
        if isinstance(status, dict):
            events.append(
                self.make_event(
                    int(changeset_id),
                    "changeset_status",
                    payload=status,
                    ts=ts,
                )
            )

        events.append(
            self.make_event(
                int(changeset_id),
                "log",
                payload={
                    "level": message.record["level"].name,
                    "message": message.record["message"],
                },
                ts=ts,
            )
        )
        for event in events:
            self.loop.call_soon_threadsafe(self._append, int(changeset_id), event)

    def make_event(
        self,
        changeset_id: int,
        event: str,
        *,
        payload: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> dict[str, Any]:
        timestamp = ts or datetime.now(timezone.utc).isoformat()
        return {
            "event": event,
            "changeset_id": changeset_id,
            "ts": timestamp,
            "payload": payload or {},
        }

    def emit(
        self,
        changeset_id: int,
        event: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self.loop is None:
            return
        message = self.make_event(changeset_id, event, payload=payload)
        self.loop.call_soon_threadsafe(self._append, changeset_id, message)

    def _append(self, changeset_id: int, event: dict[str, Any]) -> None:
        buf = self.buffers.setdefault(changeset_id, deque(maxlen=self.max_events))
        buf.append(event)
        for queue in self.listeners.get(changeset_id, set()):
            queue.put_nowait(event)

    def subscribe(
        self, changeset_id: int
    ) -> tuple[list[dict[str, Any]], asyncio.Queue[dict[str, Any]]]:
        buffer = self.buffers.setdefault(changeset_id, deque(maxlen=self.max_events))
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.listeners.setdefault(changeset_id, set()).add(queue)
        return list(buffer), queue

    def unsubscribe(self, changeset_id: int, queue: asyncio.Queue[dict[str, Any]]) -> None:
        listeners = self.listeners.get(changeset_id)
        if listeners and queue in listeners:
            listeners.remove(queue)
            if not listeners:
                self.listeners.pop(changeset_id, None)

    def get_buffer(self, changeset_id: int) -> list[dict[str, Any]]:
        return list(self.buffers.get(changeset_id, []))
