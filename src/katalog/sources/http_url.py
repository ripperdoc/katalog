from __future__ import annotations

import email.utils as email_utils
import hashlib
import logging
from dataclasses import dataclass
from datetime import timedelta, timezone
from typing import Any, Literal
import secrets

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import (
    DATA_FILE_READER,
    FILE_SIZE,
    FILE_TYPE,
    FILE_URI,
    TIME_MODIFIED,
)
from katalog.models import Asset, Actor, DataReader, MetadataChanges, OpStatus
from katalog.models import MetadataKey
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.blob_cache import get_cached_blob, put_cached_blob
from katalog.utils.url import canonicalize_web_url


@dataclass
class CrawlResponse:
    loaded_url: str
    headers: dict[str, str]
    content: bytes


class HttpDataReader(DataReader):
    """HTTP reader that fetches content through Crawlee and caches by URL digest."""

    def __init__(self, source: "HttpUrlSource", url: str):
        self.source = source
        self.url = url
        self._cache_digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        self._cached_content: bytes | None = None

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        if self._cached_content is None and not no_cache:
            cached = get_cached_blob(hash_type="url", digest=self._cache_digest)
            if cached is not None:
                self._cached_content = cached

        if self._cached_content is None or no_cache:
            response = await self.source._crawl_once(self.url, method="GET")
            if response is None:
                self._cached_content = b""
            else:
                self._cached_content = response.content
                if not no_cache:
                    put_cached_blob(
                        hash_type="url",
                        digest=self._cache_digest,
                        data=self._cached_content,
                    )
        data = self._cached_content
        if data is None:
            return b""
        if offset < 0:
            offset = 0
        if length is None:
            return data[offset:]
        return data[offset : offset + length]


class HttpUrlSource(SourcePlugin):
    """Source that can recurse into HTTP/HTTPS URL assets."""

    plugin_id = "katalog.sources.http_url.HttpUrlSource"
    title = "HTTP URL"
    description = "Fetch metadata for HTTP/HTTPS assets and provide data reader access."

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")
        timeout_seconds: float = Field(default=30.0, gt=0)
        user_agent: str = Field(default="katalog/0.1")
        max_request_retries: int = Field(default=1, ge=0)
        verbose_crawler_logs: bool = Field(
            default=False,
            description="Enable verbose Crawlee retry/failure logs for HTTP requests.",
        )

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.timeout_seconds = cfg.timeout_seconds
        self.user_agent = cfg.user_agent
        self.max_request_retries = cfg.max_request_retries
        self.verbose_crawler_logs = cfg.verbose_crawler_logs

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "HTTP URL source",
            "version": "0.1",
        }

    def authorize(self, **kwargs) -> str:
        _ = kwargs
        return ""

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        _ = key
        asset = changes.asset
        if asset is None:
            return None
        url = self._asset_url(asset)
        if not url:
            return None
        return HttpDataReader(source=self, url=url)

    def get_namespace(self) -> str:
        return "web"

    def can_scan_uri(self, uri: str) -> bool:
        return uri.startswith("http://") or uri.startswith("https://")

    async def scan(self) -> ScanResult:
        # This source is intended to be used as a recursive source.
        async def _empty_iterator():
            if False:  # pragma: no cover
                yield

        return ScanResult(iterator=_empty_iterator(), status=OpStatus.COMPLETED, ignored=0)

    def can_scan_asset(self, changes: MetadataChanges) -> int:
        url = self._changes_url(changes)
        if not url:
            return 0
        if self.can_scan_uri(url):
            return 50
        return 0

    async def scan_from_asset(self, changes: MetadataChanges) -> ScanResult:
        asset = changes.asset
        if asset is None:
            return await self.scan()
        url = self._changes_url(changes)
        if not url:
            return await self.scan()

        async def _iterator():
            response = await self._crawl_once(url, method="HEAD")
            final_url = (
                canonicalize_web_url(response.loaded_url)
                if response is not None
                else canonicalize_web_url(url)
            )

            emitted = Asset(
                namespace=asset.namespace,
                external_id=asset.external_id,
                canonical_uri=final_url,
                actor_id=self.actor.id,
            )
            result = AssetScanResult(asset=emitted, actor=self.actor)
            result.set_metadata(FILE_URI, final_url)
            result.set_metadata(DATA_FILE_READER, {})

            if response is not None:
                content_type = response.headers.get("content-type", "").split(";", 1)[0]
                if content_type:
                    result.set_metadata(FILE_TYPE, content_type)
                content_length = response.headers.get("content-length")
                if content_length and content_length.isdigit():
                    result.set_metadata(FILE_SIZE, int(content_length))
                modified = self._parse_http_datetime(response.headers.get("last-modified"))
                if modified is not None:
                    result.set_metadata(TIME_MODIFIED, modified)
            yield result

        return ScanResult(iterator=_iterator(), status=OpStatus.COMPLETED, ignored=0)

    async def _crawl_once(
        self, url: str, *, method: Literal["GET", "HEAD"]
    ) -> CrawlResponse | None:
        result: CrawlResponse | None = None

        request = Request.from_url(
            url,
            method=method,
            headers={"User-Agent": self.user_agent},
        )
        storage_client = MemoryStorageClient()
        request_queue = await RequestQueue.open(
            alias=f"http-url-{secrets.token_hex(4)}",
            storage_client=storage_client,
        )
        crawler_logger: logging.Logger | None = None
        if not self.verbose_crawler_logs:
            crawler_logger = logging.getLogger(
                f"katalog.sources.http_url.crawler.{self.actor.id or 'unknown'}"
            )
            crawler_logger.disabled = False
            crawler_logger.setLevel(logging.CRITICAL + 1)
            crawler_logger.propagate = False
        crawler = HttpCrawler(
            storage_client=storage_client,
            request_manager=request_queue,
            max_request_retries=self.max_request_retries,
            use_session_pool=False,
            retry_on_blocked=False,
            configure_logging=self.verbose_crawler_logs,
            navigation_timeout=timedelta(seconds=self.timeout_seconds),
            _logger=crawler_logger,
        )

        @crawler.router.default_handler
        async def request_handler(context: HttpCrawlingContext) -> None:
            nonlocal result
            response = context.http_response
            headers = {str(k).lower(): str(v) for k, v in response.headers.items()}
            loaded_url = context.request.loaded_url or context.request.url
            content = b""
            if method != "HEAD":
                content = await response.read()
            result = CrawlResponse(
                loaded_url=str(loaded_url),
                headers=headers,
                content=content,
            )

        try:
            await crawler.run([request])
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "HTTP crawl failed method={method} url={url} error={error}",
                method=method,
                url=url,
                error=str(exc),
            )
            return None
        finally:
            try:
                request_manager = await crawler.get_request_manager()
                await request_manager.drop()
            except Exception:
                # Request queue cleanup should not break processing.
                pass
        if result is None:
            logger.warning(
                "HTTP crawl returned no response method={method} url={url}",
                method=method,
                url=url,
            )
        return result

    @staticmethod
    def _changes_url(changes: MetadataChanges) -> str | None:
        entries = changes.entries_for_key(FILE_URI)
        for entry in entries:
            value = entry.value
            if isinstance(value, str) and value:
                return canonicalize_web_url(value)
        asset = changes.asset
        if asset is None:
            return None
        return HttpUrlSource._asset_url(asset)

    @staticmethod
    def _asset_url(asset: Asset) -> str | None:
        candidate = asset.canonical_uri or ""
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return canonicalize_web_url(candidate)
        if asset.external_id.startswith("http://") or asset.external_id.startswith("https://"):
            return canonicalize_web_url(asset.external_id)
        return None

    @staticmethod
    def _parse_http_datetime(value: str | None):
        if not value:
            return None
        try:
            dt = email_utils.parsedate_to_datetime(value)
        except Exception:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
