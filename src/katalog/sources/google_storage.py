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
from katalog.models import Asset, Actor, DataReader, MetadataChanges, MetadataKey, OpStatus
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.blob_cache import get_cached_blob, put_cached_blob
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
        hash_md5: str | None = None,
    ):
        self.source = source
        self.bucket = bucket
        self.object_name = object_name
        self.hash_md5 = (hash_md5 or "").strip().lower() or None

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        if offset < 0:
            offset = 0
        if length is not None and length <= 0:
            return b""

        # Resolve cache by stable content hash when available.
        if not no_cache and self.hash_md5:
            cached = get_cached_blob(hash_type="md5", digest=self.hash_md5)
            if cached is not None:
                if length is None:
                    return cached[offset:]
                return cached[offset : offset + length]

        data = await asyncio.to_thread(
            self.source._read_object_sync,
            self.bucket,
            self.object_name,
            offset,
            length,
        )
        if (
            not no_cache
            and self.hash_md5
            and offset == 0
            and length is None
            and isinstance(data, bytes)
        ):
            put_cached_blob(hash_type="md5", digest=self.hash_md5, data=data)
        return data


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

    async def get_data_reader(
        self, key: MetadataKey, changes: MetadataChanges
    ) -> DataReader | None:
        _ = key
        asset = changes.asset
        if asset is None:
            return None
        bucket = self.bucket
        object_name = self._asset_object_name(asset)
        if not object_name:
            return None
        hash_md5 = None
        raw_hash = changes.latest_value(HASH_MD5, value_type=str)
        if raw_hash and raw_hash.strip():
            hash_md5 = raw_hash.strip().lower()
        return GoogleStorageDataReader(
            source=self,
            bucket=bucket,
            object_name=object_name,
            hash_md5=hash_md5,
        )

    async def scan(self) -> ScanResult:
        ignored = 0
        status = OpStatus.IN_PROGRESS

        async def iterator():
            nonlocal ignored, status
            seen = 0
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
                result.set_metadata(
                    DATA_FILE_READER,
                    {},
                )
                if blob.size is not None:
                    result.set_metadata(FILE_SIZE, int(blob.size))
                if blob.content_type:
                    result.set_metadata(FILE_TYPE, str(blob.content_type))
                if blob.updated is not None:
                    result.set_metadata(TIME_MODIFIED, blob.updated)
                md5_hex = _safe_md5_hex(blob.md5_hash)
                if md5_hex:
                    result.set_metadata(HASH_MD5, md5_hex)
                yield result
                seen += 1
                if self.max_files and seen >= self.max_files:
                    status = OpStatus.PARTIAL
                    break

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
    ) -> bytes:
        client = self._get_client_sync()
        blob = client.bucket(bucket).blob(object_name)
        end = None if length is None else offset + length - 1
        return blob.download_as_bytes(start=offset, end=end)


def _next_page_or_none(pages):
    try:
        return next(pages)
    except StopIteration:
        return None
