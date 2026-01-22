import asyncio
from collections import deque
from loguru import logger


def sse_event(event: str, data: str) -> str:
    payload = "\n".join(f"data: {line}" for line in data.splitlines())
    return f"event: {event}\n{payload}\n\n"


class ChangesetEventManager:
    """Keep a small in-memory buffer of changeset logs and stream new ones."""

    def __init__(self, *, max_events: int = 200):
        self.max_events = max_events
        self.buffers: dict[int, deque[str]] = {}
        self.listeners: dict[int, set[asyncio.Queue[str]]] = {}
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
        changeset_id = message.record["extra"].get("changeset_id")
        if changeset_id is None:
            return
        ts = message.record["time"].strftime("%Y-%m-%d %H:%M:%S")
        text = f"[{ts}] {message.record['level'].name}: {message.record['message']}"
        if self.loop is None:
            return
        self.loop.call_soon_threadsafe(self._append, int(changeset_id), text)

    def _append(self, changeset_id: int, text: str) -> None:
        buf = self.buffers.setdefault(changeset_id, deque(maxlen=self.max_events))
        buf.append(text)
        for queue in self.listeners.get(changeset_id, set()):
            queue.put_nowait(text)

    def subscribe(self, changeset_id: int) -> tuple[list[str], asyncio.Queue[str]]:
        buffer = self.buffers.setdefault(changeset_id, deque(maxlen=self.max_events))
        queue: asyncio.Queue[str] = asyncio.Queue()
        self.listeners.setdefault(changeset_id, set()).add(queue)
        return list(buffer), queue

    def unsubscribe(self, changeset_id: int, queue: asyncio.Queue[str]) -> None:
        listeners = self.listeners.get(changeset_id)
        if listeners and queue in listeners:
            listeners.remove(queue)
            if not listeners:
                self.listeners.pop(changeset_id, None)

    def get_buffer(self, changeset_id: int) -> list[str]:
        return list(self.buffers.get(changeset_id, []))
