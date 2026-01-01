from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    MutableMapping,
)
from loguru import logger

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)


@dataclass(slots=True)
class RequestSpec:
    """Lightweight request descriptor used to drive slice and page fetching."""

    method: str
    url: str
    params: Mapping[str, Any] | None = None
    headers: Mapping[str, str] | None = None
    json: Any | None = None
    data: Any | None = None
    content: Any | None = None
    log_line: str | None = None

    def as_kwargs(self) -> MutableMapping[str, Any]:
        """Return a dict suitable for httpx.AsyncClient.request(**kwargs)."""

        kwargs: MutableMapping[str, Any] = {"method": self.method, "url": self.url}
        if self.params:
            kwargs["params"] = self.params
        if self.headers:
            kwargs["headers"] = self.headers
        if self.json is not None:
            kwargs["json"] = self.json
        if self.data is not None:
            kwargs["data"] = self.data
        if self.content is not None:
            kwargs["content"] = self.content
        return kwargs


NextPageBuilder = Callable[
    [RequestSpec, httpx.Response], Awaitable[RequestSpec | None] | RequestSpec | None
]


@dataclass(slots=True)
class _QueueItem:
    value: httpx.Response | None = None
    error: BaseException | None = None
    done: bool = False


def default_retrying() -> AsyncRetrying:
    return AsyncRetrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=8.0),
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.TransportError, httpx.RemoteProtocolError)
        ),
        reraise=True,
    )


class ConcurrentSliceFetcher:
    """
    Execute slice-based REST requests concurrently while respecting pagination.

    - Accepts a slice generator (iterable or async iterable) that yields RequestSpec objects.
    - Runs up to `concurrency` slices in flight. Each slice can span multiple pages, driven
      by `next_page`, which receives the previous RequestSpec and httpx.Response.
    - Retries individual requests using tenacity (configurable via the `retrying` argument).
    - Emits results as an unordered AsyncIterator of httpx.Response.
    - Supports fast cancellation: cancelling the consumer or calling `cancel()` stops all tasks.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        concurrency: int = 5,
        retrying: AsyncRetrying | None = None,
        timeout: httpx.Timeout | float | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None
        self._retrying = retrying or default_retrying()
        self._concurrency = concurrency
        self._cancel_event = asyncio.Event()

    async def __aenter__(self) -> "ConcurrentSliceFetcher":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        await self.aclose()

    async def aclose(self) -> None:
        self.cancel()
        if self._owns_client:
            await self._client.aclose()

    def cancel(self) -> None:
        self._cancel_event.set()

    async def stream(
        self,
        slices: Iterable[RequestSpec] | AsyncIterable[RequestSpec],
        *,
        next_page: NextPageBuilder | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """
        Stream combined responses or parsed items from concurrent slice fetches.

        Cancelling the consumer or calling `cancel()` will attempt to stop all in-flight tasks.
        Raises the first encountered exception after cancelling the rest.
        """

        queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        tasks: set[asyncio.Task[None]] = set()
        semaphore = asyncio.Semaphore(self._concurrency)
        active_workers = 0
        active_lock = asyncio.Lock()
        producer_finished = asyncio.Event()

        async def worker(spec: RequestSpec) -> None:
            nonlocal active_workers
            try:
                async for item in self._stream_slice(spec, next_page):
                    if self._cancel_event.is_set():
                        break
                    await queue.put(_QueueItem(value=item))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                await queue.put(_QueueItem(error=exc))
                self.cancel()
            finally:
                async with active_lock:
                    active_workers -= 1
                    should_signal_done = (
                        producer_finished.is_set() and active_workers == 0
                    )
                semaphore.release()
                if should_signal_done:
                    await queue.put(_QueueItem(done=True))

        async def spawn_workers() -> None:
            nonlocal active_workers
            try:
                async for spec in _iter_slices(slices):
                    if self._cancel_event.is_set():
                        break
                    await semaphore.acquire()
                    async with active_lock:
                        active_workers += 1
                    task = asyncio.create_task(worker(spec))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
            finally:
                producer_finished.set()
                async with active_lock:
                    should_signal_done = active_workers == 0
                if should_signal_done:
                    await queue.put(_QueueItem(done=True))

        self._cancel_event = asyncio.Event()
        producer_task = asyncio.create_task(spawn_workers())
        try:
            while True:
                item = await queue.get()
                if item.error:
                    self.cancel()
                    raise item.error
                if item.done:
                    break
                assert item.value is not None
                yield item.value
        except asyncio.CancelledError:
            self.cancel()
            raise
        finally:
            self.cancel()
            producer_task.cancel()
            for task in list(tasks):
                task.cancel()
            await asyncio.gather(producer_task, *tasks, return_exceptions=True)
            await self.aclose()

    async def _stream_slice(
        self,
        initial_spec: RequestSpec,
        next_page: NextPageBuilder | None,
    ) -> AsyncIterator[httpx.Response]:
        spec: RequestSpec | None = initial_spec
        page = 1
        while spec and not self._cancel_event.is_set():
            response = await self._send_with_retry(spec, page=page)
            yield response
            if next_page is None:
                break
            spec = await _maybe_await(next_page(spec, response))
            page += 1

    async def _send_with_retry(
        self, spec: RequestSpec, *, page: int | None = None
    ) -> httpx.Response:
        async for attempt in self._retrying:
            with attempt:
                log_line = (spec.log_line or "") + (f" [page {page}]" if page else "")
                logger.debug(f"fetch {spec.method} {spec.url}{log_line}")
                try:
                    response = await self._client.request(**spec.as_kwargs())
                    response.raise_for_status()
                    return response
                except httpx.HTTPStatusError as exc:
                    body = exc.response.text
                    logger.error(
                        f"HTTP {exc.response.status_code} for {spec.method} {spec.url} {log_line} "
                        f"body={body[:500]}"
                    )
                    raise
        raise RuntimeError("retry loop terminated unexpectedly")


async def _iter_slices(
    slices: Iterable[RequestSpec] | AsyncIterable[RequestSpec],
) -> AsyncIterator[RequestSpec]:
    if isinstance(slices, AsyncIterable):
        async for item in slices:
            yield item
    else:
        for item in slices:
            yield item


async def _maybe_await(value: Awaitable[Any] | Any) -> Any:
    if inspect.isawaitable(value):
        return await value  # type: ignore[misc]
    return value
