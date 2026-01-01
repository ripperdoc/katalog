import asyncio

import httpx
import pytest
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from katalog.utils.concurrent_fetcher import ConcurrentSliceFetcher, RequestSpec


def _mock_transport(responses: dict[str, httpx.Response | Exception]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        entry = responses[str(request.url)]
        if isinstance(entry, Exception):
            raise entry
        return entry

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_pagination_with_next_page() -> None:
    responses = {
        "https://api.example.com/page1": httpx.Response(
            200, json={"items": [1], "next": "https://api.example.com/page2"}
        ),
        "https://api.example.com/page2": httpx.Response(200, json={"items": [2], "next": None}),
    }
    transport = _mock_transport(responses)

    async with httpx.AsyncClient(transport=transport) as client, ConcurrentSliceFetcher(
        client=client, concurrency=2
    ) as fetcher:
        items: list[int] = []

        async for response in fetcher.stream(
            slices=[RequestSpec("GET", "https://api.example.com/page1")],
            next_page=lambda spec, resp: RequestSpec(
                spec.method,
                resp.json()["next"],
            )
            if resp.json().get("next")
            else None,
        ):
            items.extend(response.json()["items"])

    assert items == [1, 2]


@pytest.mark.asyncio
async def test_stream_raises_on_first_error_and_cancels_rest() -> None:
    error = httpx.TransportError("boom")
    responses = {
        "https://api.example.com/fail": error,
        "https://api.example.com/ok": httpx.Response(200, json={"items": [123]}),
    }
    transport = _mock_transport(responses)
    # Stop after first attempt to avoid long retry waits in the test.
    retrying = AsyncRetrying(
        stop=stop_after_attempt(1),
        wait=wait_fixed(0),
        retry=retry_if_exception_type(httpx.TransportError),
        reraise=True,
    )

    async with httpx.AsyncClient(transport=transport) as client, ConcurrentSliceFetcher(
        client=client, concurrency=2, retrying=retrying
    ) as fetcher:
        stream = fetcher.stream(
            slices=[
                RequestSpec("GET", "https://api.example.com/fail"),
                RequestSpec("GET", "https://api.example.com/ok"),
            ]
        )
        with pytest.raises(httpx.TransportError):
            async for _ in stream:
                # Give other tasks a chance to run to ensure cancellation propagates.
                await asyncio.sleep(0)
