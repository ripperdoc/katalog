from __future__ import annotations

import asyncio
import base64
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from google.auth import default as google_auth_default
from google.cloud import storage
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, model_validator

from katalog.constants.metadata import (
    DATA_FILE_READER,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    FILE_TYPE,
    FILE_URI,
    HASH_MD5,
    TIME_MODIFIED,
)
from katalog.models import Asset, Actor, DataReader, OpStatus
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.sources.sidecars import (
    SidecarSupport,
)
from katalog.utils.utils import match_paths, normalize_glob_patterns


def parse_gcs_url(value: str) -> tuple[str, str]:
    raw = value.strip()
    if not raw:
        raise ValueError("gcs_url is required")
    if raw.startswith("gs://"):
        without_scheme = raw[5:]
        bucket, _, prefix = without_scheme.partition("/")
        if not bucket:
            raise ValueError("Invalid gs:// URL, missing bucket")
        return bucket, prefix.strip("/")

    parsed = urlparse(raw)
    host = parsed.netloc.lower()
    if host not in {"storage.googleapis.com", "storage.cloud.google.com"}:
        raise ValueError(
            "gcs_url must be gs://... or https://storage.googleapis.com/... "
            "or https://storage.cloud.google.com/..."
        )
    path = parsed.path.lstrip("/")
    bucket, _, prefix = path.partition("/")
    if not bucket:
        raise ValueError("Invalid GCS URL, missing bucket")
    return bucket, prefix.strip("/")


def _gcs_object_uri(bucket: str, name: str) -> str:
    return f"gs://{bucket}/{name}"


def _canonical_object_uri(bucket: str, name: str) -> str:
    return f"https://storage.googleapis.com/{bucket}/{name}"


def _safe_md5_hex(md5_hash_b64: str | None) -> str | None:
    if not md5_hash_b64:
        return None
    try:
        return base64.b64decode(md5_hash_b64).hex()
    except Exception:
        return None


class GoogleStorageDataReader(DataReader):
    def __init__(
        self,
        *,
        source: "GoogleStorageSource",
        bucket: str,
        object_name: str,
        generation: int | None = None,
    ):
        self.source = source
        self.bucket = bucket
        self.object_name = object_name
        self.generation = generation

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        _ = no_cache
        if offset < 0:
            offset = 0
        if length is not None and length <= 0:
            return b""
        return await asyncio.to_thread(
            self.source._read_object_sync,
            self.bucket,
            self.object_name,
            offset,
            length,
            self.generation,
        )


class GoogleStorageSource(SourcePlugin):
    plugin_id = "katalog.sources.google_storage.GoogleStorageSource"
    title = "Google Cloud Storage"
    description = "Scan a Google Cloud Storage bucket/prefix using ADC credentials."

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        gcs_url: str = Field(
            ...,
            description="GCS root URL, e.g. gs://my-bucket or gs://my-bucket/path/prefix",
        )
        max_files: int = Field(default=0, ge=0, description="0 means no limit")
        recursive: bool = Field(default=True)
        include_paths: list[str] = Field(default_factory=list)
        exclude_paths: list[str] = Field(default_factory=list)
        project: str | None = Field(default=None)
        enable_sidecars: bool = Field(
            default=True,
            description="Parse known sidecar files (.truth.md/.queries.yml/.summary.md) as metadata.",
        )

        @model_validator(mode="after")
        def _validate_gcs_url(self) -> "GoogleStorageSource.ConfigModel":
            parse_gcs_url(self.gcs_url)
            return self

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        bucket, prefix = parse_gcs_url(cfg.gcs_url)
        self.bucket = bucket
        self.prefix = prefix
        self.max_files = cfg.max_files
        self.recursive = cfg.recursive
        self.include_paths = normalize_glob_patterns(cfg.include_paths)
        self.exclude_paths = normalize_glob_patterns(cfg.exclude_paths)
        self.project = cfg.project
        self.enable_sidecars = bool(cfg.enable_sidecars)
        self._client: storage.Client | None = None

    def get_info(self) -> dict[str, Any]:
        return {"description": "Google Cloud Storage source", "version": "0.1"}

    def authorize(self, **kwargs) -> str:
        _ = kwargs
        return ""

    def get_namespace(self) -> str:
        return f"gcs:{self.bucket}"

    def can_scan_uri(self, uri: str) -> bool:
        try:
            parse_gcs_url(uri)
            return True
        except Exception:
            return False

    async def is_ready(self) -> tuple[bool, str | None]:
        try:
            google_auth_default(
                scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
            )
        except Exception as exc:  # noqa: BLE001
            return False, f"ADC credentials unavailable: {exc}"
        try:
            await asyncio.to_thread(self._probe_access_sync)
        except Exception as exc:  # noqa: BLE001
            return False, f"GCS access check failed: {exc}"
        return True, None

    def get_data_reader(self, asset: Asset, params: dict[str, Any] | None = None) -> Any:
        data = params or {}
        bucket = str(data.get("bucket") or self.bucket)
        object_name = str(data.get("object_name") or "").strip("/")
        generation_raw = data.get("generation")
        generation = int(generation_raw) if generation_raw is not None else None
        if not object_name:
            object_name = self._asset_object_name(asset)
        if not object_name:
            return None
        return GoogleStorageDataReader(
            source=self,
            bucket=bucket,
            object_name=object_name,
            generation=generation,
        )

    async def scan(self) -> ScanResult:
        ignored = 0
        status = OpStatus.IN_PROGRESS

        async def iterator():
            nonlocal ignored, status
            seen = 0
            sidecars = SidecarSupport.create(enabled=self.enable_sidecars)
            async for blob in self._iterate_blobs():
                object_name = str(blob.name or "")
                if not object_name:
                    ignored += 1
                    continue
                file_name = PurePosixPath(object_name).name
                if not match_paths(
                    paths=(object_name, file_name),
                    include=self.include_paths,
                    exclude=self.exclude_paths,
                ):
                    ignored += 1
                    continue
                if sidecars.is_candidate(object_name):
                    try:
                        raw = await asyncio.to_thread(
                            self._read_object_sync,
                            self.bucket,
                            object_name,
                            0,
                            None,
                            None,
                        )
                    except Exception as exc:
                        ignored += 1
                        logger.warning(
                            "Failed to read sidecar {name} for source {actor_id}: {err}",
                            name=object_name,
                            actor_id=self.actor.id,
                            err=exc,
                        )
                        continue
                    consumed, emitted = sidecars.consume_candidate(
                        path_or_name=object_name,
                        actor=self.actor,
                        text=raw.decode("utf-8", errors="replace"),
                    )
                    if consumed and emitted is not None:
                        yield emitted
                    continue

                canonical_uri = _canonical_object_uri(self.bucket, object_name)
                asset = Asset(
                    external_id=object_name,
                    namespace=self.get_namespace(),
                    canonical_uri=canonical_uri,
                    actor_id=self.actor.id,
                )
                result = AssetScanResult(asset=asset, actor=self.actor)
                result.set_metadata(FILE_URI, _gcs_object_uri(self.bucket, object_name))
                result.set_metadata(FILE_PATH, object_name)
                result.set_metadata(FILE_NAME, file_name)
                result.set_metadata(DATA_FILE_READER, {
                    "bucket": self.bucket,
                    "object_name": object_name,
                    "generation": int(blob.generation) if blob.generation else None,
                })
                if blob.size is not None:
                    result.set_metadata(FILE_SIZE, int(blob.size))
                if blob.content_type:
                    result.set_metadata(FILE_TYPE, str(blob.content_type))
                if blob.updated is not None:
                    result.set_metadata(TIME_MODIFIED, blob.updated)
                md5_hex = _safe_md5_hex(blob.md5_hash)
                if md5_hex:
                    result.set_metadata(HASH_MD5, md5_hex)
                sidecars.apply_deferred(asset=asset, result=result)
                yield result
                seen += 1
                if self.max_files and seen >= self.max_files:
                    status = OpStatus.PARTIAL
                    break

            sidecars.log_unresolved(source_name="GoogleStorage", actor_id=self.actor.id)

            if status == OpStatus.IN_PROGRESS:
                status = OpStatus.COMPLETED
            scan_result.status = status
            scan_result.ignored = ignored

        scan_result = ScanResult(iterator=iterator(), status=status, ignored=ignored)
        return scan_result

    async def _iterate_blobs(self):
        pages = await asyncio.to_thread(self._open_blob_pages_sync)
        while True:
            page = await asyncio.to_thread(_next_page_or_none, pages)
            if page is None:
                break
            blobs = await asyncio.to_thread(lambda: list(page))
            for blob in blobs:
                yield blob

    def _open_blob_pages_sync(self):
        client = self._get_client_sync()
        kwargs: dict[str, Any] = {}
        if self.prefix:
            kwargs["prefix"] = f"{self.prefix.rstrip('/')}/"
        if not self.recursive:
            kwargs["delimiter"] = "/"
        return client.list_blobs(self.bucket, **kwargs).pages

    def _probe_access_sync(self) -> None:
        client = self._get_client_sync()
        iterator = client.list_blobs(
            self.bucket,
            prefix=f"{self.prefix.rstrip('/')}/" if self.prefix else None,
            max_results=1,
        )
        # Trigger at least one request to validate credentials and bucket access.
        list(iterator)

    def _get_client_sync(self) -> storage.Client:
        if self._client is not None:
            return self._client
        self._client = storage.Client(project=self.project)
        return self._client

    def _asset_object_name(self, asset: Asset) -> str:
        if asset.namespace == self.get_namespace():
            return asset.external_id.strip("/")
        uri = asset.canonical_uri or ""
        if uri.startswith("gs://"):
            bucket, object_name = parse_gcs_url(uri)
            if bucket == self.bucket:
                return object_name
        return ""

    def _read_object_sync(
        self,
        bucket: str,
        object_name: str,
        offset: int,
        length: int | None,
        generation: int | None,
    ) -> bytes:
        client = self._get_client_sync()
        blob = client.bucket(bucket).blob(object_name, generation=generation)
        end = None if length is None else offset + length - 1
        return blob.download_as_bytes(start=offset, end=end)


def _next_page_or_none(pages):
    try:
        return next(pages)
    except StopIteration:
        return None
