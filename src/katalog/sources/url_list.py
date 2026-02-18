from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.models import (
    Asset,
    Actor,
    DataReader,
    MetadataChanges,
    MetadataKey,
    OpStatus,
)
from katalog.constants.metadata import FILE_URI
from katalog.utils.url import canonicalize_web_url


class UrlListSource(SourcePlugin):
    """Source that emits assets from configured URLs or from a URL list file."""

    plugin_id = "katalog.sources.url_list.UrlListSource"
    title = "URL list"
    description = "Emit web assets from a static list of URLs."

    class ConfigModel(BaseModel):
        namespace: str = Field(default="web")
        urls: list[str] = Field(default_factory=list)
        url_file: str | None = Field(
            default=None, description="Workspace-relative or absolute text file with one URL per line."
        )
        max_urls: int = Field(default=0, ge=0, description="0 means no limit")

        @model_validator(mode="after")
        def _validate_source(self) -> "UrlListSource.ConfigModel":
            if not self.urls and not self.url_file:
                raise ValueError("Provide either urls or url_file")
            return self

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.namespace = cfg.namespace
        self.urls = [self._normalize_url(url) for url in cfg.urls]
        self.url_file = cfg.url_file
        self.max_urls = cfg.max_urls

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "URL list source",
            "version": "0.1",
        }

    def authorize(self, **kwargs) -> str:
        _ = kwargs
        return ""

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        _ = key, changes
        return None

    def can_scan_uri(self, uri: str) -> bool:
        _ = uri
        return True

    def get_namespace(self) -> str:
        return self.namespace

    async def scan(self) -> ScanResult:
        urls = self._collect_urls()
        if self.max_urls > 0:
            urls = urls[: self.max_urls]
        valid_urls: list[str] = []
        ignored = 0
        for raw_url in urls:
            url = self._normalize_url(raw_url)
            if not (url.startswith("http://") or url.startswith("https://")):
                ignored += 1
                continue
            valid_urls.append(url)

        async def iterator():
            for url in valid_urls:
                asset = Asset(
                    namespace=self.namespace,
                    external_id=url,
                    canonical_uri=url,
                    actor_id=self.actor.id,
                )
                result = AssetScanResult(asset=asset, actor=self.actor)
                result.set_metadata(FILE_URI, url)
                yield result

        return ScanResult(iterator=iterator(), status=OpStatus.COMPLETED, ignored=ignored)

    def _collect_urls(self) -> list[str]:
        urls = list(self.urls)
        if self.url_file:
            url_file = Path(self.url_file).expanduser()
            if not url_file.is_absolute():
                url_file = (Path.cwd() / url_file).resolve()
            if url_file.exists():
                lines = url_file.read_text(encoding="utf-8").splitlines()
                urls.extend(lines)
        seen: set[str] = set()
        deduped: list[str] = []
        for url in urls:
            normalized = self._normalize_url(url)
            if not normalized or normalized.startswith("#"):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _normalize_url(value: str) -> str:
        return canonicalize_web_url(value)
