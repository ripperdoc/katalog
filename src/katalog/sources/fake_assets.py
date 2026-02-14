from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Any, AsyncIterator, Dict, Iterable
from urllib.parse import parse_qs, urlparse

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from katalog.constants.metadata import (
    ACCESS_LAST_MODIFYING_USER,
    ACCESS_OWNER,
    ACCESS_SHARED_WITH,
    ACCESS_SHARING_USER,
    COLLECTION_MEMBER,
    DATA_FILE_READER,
    DOC_AUTHOR,
    DOC_LANG,
    DOC_PAGES,
    DOC_SUMMARY,
    DOC_WORDS,
    FILE_COMMENT,
    FILE_DESCRIPTION,
    FILE_DOWNLOAD_URI,
    FILE_EXTENSION,
    FILE_ID_PATH,
    FILE_NAME,
    FILE_PATH,
    FILE_SIZE,
    FILE_TAGS,
    FILE_TITLE,
    FILE_TYPE,
    FILE_URI,
    FILE_VERSION,
    FLAG_FAVORITE,
    FLAG_HIDDEN,
    FLAG_SHARED,
    FLAG_TRASHED,
    HASH_MD5,
    HASH_MINHASH,
    HASH_SHA1,
    HASH_SIMHASH,
    IMAGE_APERTURE,
    IMAGE_GPS_LATITUDE,
    IMAGE_GPS_LONGITUDE,
    IMAGE_ISO,
    REL_DERIVED_FROM,
    TIME_ACCESSED,
    TIME_BIRTHTIME,
    TIME_CREATED,
    TIME_DOWNLOADED,
    TIME_MODIFIED,
    TIME_TRASHED,
    MetadataType,
    get_metadata_id,
)
from katalog.models import (
    Asset,
    AssetCollection,
    DataReader,
    Metadata,
)
from katalog.models import Actor, OpStatus
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.db.asset_collections import get_asset_collection_repo


class _FakeAssetSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    external_id: str
    canonical_uri: str
    file_name: str
    file_path: str
    file_size: int
    file_type: str
    extension: str


class FakeAssetReader(DataReader):
    """Deterministic byte generator for synthetic assets."""

    def __init__(self, *, size: int, seed: int, token: str) -> None:
        self.size = max(0, int(size))
        self.seed = int(seed)
        self.token = token

    async def read(
        self, offset: int = 0, length: int | None = None, no_cache: bool = False
    ) -> bytes:
        if offset < 0:
            return b""
        if length is None or length < 0:
            length = self.size - offset
        if length <= 0 or offset >= self.size:
            return b""
        length = min(length, self.size - offset)

        block_size = 4096
        start_block = offset // block_size
        end_block = (offset + length - 1) // block_size
        parts: list[bytes] = []
        for block in range(start_block, end_block + 1):
            rng = Random(f"{self.seed}:{self.token}:{block}")
            parts.append(rng.randbytes(block_size))

        data = b"".join(parts)
        start = offset - start_block * block_size
        return data[start : start + length]


class FakeAssetSource(SourcePlugin):
    """Source that emits realistic-looking synthetic assets for demos and tests."""

    plugin_id = "katalog.sources.fake_assets.FakeAssetSource"
    title = "Fake assets"
    description = "Generate synthetic assets with rich metadata for testing."

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        namespace: str = Field(
            default="fake",
            description="Namespace for external_id uniqueness",
        )
        total_assets: int = Field(
            default=250,
            ge=0,
            description="Total number of assets to emit",
        )
        batch_size: int = Field(
            default=50,
            ge=1,
            description="Emit assets in batches of this size",
        )
        batch_delay_ms: float = Field(
            default=0,
            ge=0,
            description="Delay (ms) between batches to simulate network latency",
        )
        batch_jitter_ms: float = Field(
            default=0,
            ge=0,
            description="Random jitter (ms) added per batch",
        )
        seed: int = Field(
            default=1,
            ge=0,
            description="Seed for deterministic fake data generation",
        )
        include_collection: bool = Field(
            default=True,
            description="Attach collection metadata to emitted assets",
        )
        hidden_path_ratio: float = Field(
            default=0.02,
            ge=0,
            le=1,
            description="Probability of generating a hidden-looking path or filename",
        )
        use_queue: bool = Field(
            default=False,
            description="Emit assets through an internal queue (decouples producer/consumer)",
        )
        queue_maxsize: int = Field(
            default=0,
            ge=0,
            description="Max size for scan queue; 0 means unbounded (no backpressure)",
        )

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)

        self.total_assets = cfg.total_assets
        self.batch_size = cfg.batch_size
        self.batch_delay_ms = cfg.batch_delay_ms
        self.batch_jitter_ms = cfg.batch_jitter_ms
        self.seed = cfg.seed
        self.include_collection = cfg.include_collection
        self.namespace = cfg.namespace
        self.use_queue = cfg.use_queue
        self.queue_maxsize = cfg.queue_maxsize
        self.hidden_path_ratio = cfg.hidden_path_ratio

        self._collection: AssetCollection | None = None

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Synthetic asset generator",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def authorize(self, **kwargs: Any) -> str:
        return ""

    def get_namespace(self) -> str:
        return self.namespace

    def get_data_reader(self, asset: Asset, params: dict | None = None) -> Any:
        size = _parse_fake_size(asset.canonical_uri) or (params or {}).get("size")
        if size is None:
            size = 1024
        token = asset.external_id or asset.canonical_uri
        return FakeAssetReader(size=int(size), seed=self.seed, token=token)

    def can_scan_uri(self, uri: str) -> bool:
        return uri.startswith("fake://")

    async def scan(self) -> ScanResult:
        status = OpStatus.IN_PROGRESS
        ignored = 0

        collection = await self._ensure_collection()
        if self.actor.id is None:
            raise ValueError("FakeAssetSource actor is missing id")
        actor_id = int(self.actor.id)
        rng = Random(self.seed + actor_id)
        scan_result: ScanResult | None = None
        logger.info(
            "FakeAssetSource scan starting for actor {actor_id} (total={total}, batch_size={batch_size})",
            actor_id=actor_id,
            total=self.total_assets,
            batch_size=self.batch_size,
        )

        async def result_stream() -> AsyncIterator[AssetScanResult]:
            nonlocal status, ignored, scan_result
            previous_asset: Asset | None = None
            emitted = 0
            batch_count = 0
            batch_index = 0
            total_assets = max(0, self.total_assets)
            for idx in range(self.total_assets):
                spec = _generate_asset_spec(
                    rng, actor_id, idx, hidden_path_ratio=self.hidden_path_ratio
                )
                asset = Asset(
                    external_id=spec.external_id,
                    namespace=self.get_namespace(),
                    canonical_uri=spec.canonical_uri,
                    actor_id=actor_id,
                )

                result = AssetScanResult(asset=asset, actor=self.actor)

                # Scalars via set_metadata().
                result.set_metadata(DATA_FILE_READER, {"size": spec.file_size})
                result.set_metadata(FILE_NAME, spec.file_name)
                result.set_metadata(FILE_PATH, spec.file_path)
                result.set_metadata(FILE_URI, spec.canonical_uri)
                result.set_metadata(FILE_EXTENSION, spec.extension)
                result.set_metadata(FILE_TYPE, spec.file_type)
                result.set_metadata(FILE_SIZE, spec.file_size)
                result.set_metadata(FILE_VERSION, rng.randint(1, 12))
                result.set_metadata(FILE_TITLE, _title_from_name(spec.file_name))
                result.set_metadata(FILE_DESCRIPTION, _make_description(rng, spec))
                result.set_metadata(FILE_COMMENT, _make_comment(rng))
                result.set_metadata(FILE_DOWNLOAD_URI, _download_uri(spec))

                result.set_metadata(FLAG_SHARED, int(rng.random() < 0.25))
                result.set_metadata(FLAG_FAVORITE, int(rng.random() < 0.1))
                result.set_metadata(FLAG_HIDDEN, int(rng.random() < 0.05))
                result.set_metadata(FLAG_TRASHED, int(rng.random() < 0.02))

                created = _random_datetime(rng, days_back=365 * 6)
                modified = created + timedelta(days=rng.randint(0, 365))
                accessed = modified + timedelta(days=rng.randint(0, 60))
                downloaded = accessed + timedelta(days=rng.randint(0, 7))
                birthtime = created - timedelta(days=rng.randint(0, 30))
                result.set_metadata(TIME_CREATED, created)
                result.set_metadata(TIME_MODIFIED, modified)
                result.set_metadata(TIME_ACCESSED, accessed)
                result.set_metadata(TIME_DOWNLOADED, downloaded)
                result.set_metadata(TIME_BIRTHTIME, birthtime)
                if rng.random() < 0.02:
                    result.set_metadata(TIME_TRASHED, accessed + timedelta(days=1))

                result.set_metadata(DOC_LANG, rng.choice(["en", "sv", "es", "de"]))
                result.set_metadata(DOC_AUTHOR, rng.choice(_AUTHOR_POOL))
                result.set_metadata(DOC_SUMMARY, _make_summary(rng))
                result.set_metadata(DOC_WORDS, rng.randint(200, 20000))
                result.set_metadata(DOC_PAGES, rng.randint(1, 120))

                result.set_metadata(IMAGE_GPS_LATITUDE, rng.uniform(-80.0, 80.0))
                result.set_metadata(IMAGE_GPS_LONGITUDE, rng.uniform(-170.0, 170.0))
                result.set_metadata(IMAGE_APERTURE, round(rng.uniform(1.4, 8.0), 1))
                result.set_metadata(IMAGE_ISO, rng.choice([100, 200, 400, 800, 1600]))

                result.set_metadata(HASH_MD5, _fake_hash(rng, "md5"))
                result.set_metadata(HASH_SHA1, _fake_hash(rng, "sha1"))
                result.set_metadata(HASH_SIMHASH, _fake_hash(rng, "simhash"))
                result.set_metadata(HASH_MINHASH, _fake_minhash(rng))

                # List-style helpers via set_metadata_list().
                result.set_metadata_list(FILE_ID_PATH, _fake_id_path(rng, idx))
                result.set_metadata_list(ACCESS_SHARED_WITH, _shared_with(rng))

                # JSON metadata.
                result.set_metadata(FILE_TAGS, _tags_for_type(rng, spec.file_type))

                # Access metadata.
                owner = rng.choice(_OWNER_POOL)
                result.set_metadata(ACCESS_OWNER, owner)
                result.set_metadata(ACCESS_LAST_MODIFYING_USER, owner)
                result.set_metadata(ACCESS_SHARING_USER, rng.choice(_OWNER_POOL))

                # Relation metadata (RELATION type) uses direct Metadata instances.
                target = previous_asset or asset
                relation_entry = Metadata(
                    metadata_key_id=get_metadata_id(REL_DERIVED_FROM),
                    value_type=MetadataType.RELATION,
                    actor_id=self.actor.id,
                    removed=False,
                )
                relation_entry.value_relation = target
                result.metadata.append(relation_entry)

                # Collection metadata (COLLECTION type) uses direct Metadata instances.
                if collection is not None:
                    collection_entry = Metadata(
                        metadata_key_id=get_metadata_id(COLLECTION_MEMBER),
                        value_type=MetadataType.COLLECTION,
                        actor_id=self.actor.id,
                        removed=False,
                    )
                    collection_entry.value_collection_id = collection.id
                    result.metadata.append(collection_entry)

                previous_asset = asset
                emitted += 1
                batch_count += 1
                yield result

                if batch_count >= self.batch_size:
                    batch_index += 1
                    remaining = max(0, total_assets - emitted)
                    logger.info(
                        "tasks_progress queued={queued} running={running} finished={finished} kind=files",
                        queued=remaining,
                        running=0,
                        finished=emitted,
                    )
                    logger.info(
                        "FakeAssetSource batch {batch_index} emitted {batch_count} assets (emitted={emitted}/{total})",
                        batch_index=batch_index,
                        batch_count=batch_count,
                        emitted=emitted,
                        total=self.total_assets,
                    )
                    await _sleep_batch(rng, self.batch_delay_ms, self.batch_jitter_ms)
                    batch_count = 0

            if batch_count > 0:
                batch_index += 1
                remaining = max(0, total_assets - emitted)
                logger.info(
                    "tasks_progress queued={queued} running={running} finished={finished} kind=files",
                    queued=remaining,
                    running=0,
                    finished=emitted,
                )
                logger.info(
                    "FakeAssetSource batch {batch_index} emitted {batch_count} assets (emitted={emitted}/{total})",
                    batch_index=batch_index,
                    batch_count=batch_count,
                    emitted=emitted,
                    total=self.total_assets,
                )

            status = OpStatus.COMPLETED
            if emitted == 0 and self.total_assets > 0:
                logger.warning(
                    "FakeAssetSource emitted zero assets for actor {actor_id}",
                    actor_id=self.actor.id,
                )
            if scan_result is not None:
                scan_result.status = status
            logger.info(
                "FakeAssetSource scan finished for actor {actor_id} (emitted={emitted}, ignored={ignored}, status={status})",
                actor_id=actor_id,
                emitted=emitted,
                ignored=ignored,
                status=status.value,
            )

        async def iterator() -> AsyncIterator[AssetScanResult]:
            if not self.use_queue:
                async for entry in result_stream():
                    yield entry
                return

            result_queue: asyncio.Queue[AssetScanResult | BaseException | None] = (
                asyncio.Queue(maxsize=self.queue_maxsize)
            )

            async def _produce() -> None:
                try:
                    async for entry in result_stream():
                        await result_queue.put(entry)
                except BaseException as exc:  # noqa: BLE001
                    await result_queue.put(exc)
                finally:
                    await result_queue.put(None)

            producer = asyncio.create_task(_produce())
            try:
                while True:
                    item = await result_queue.get()
                    if item is None:
                        break
                    if isinstance(item, BaseException):
                        raise item
                    yield item
            finally:
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)

        scan_result = ScanResult(iterator=iterator(), status=status, ignored=ignored)
        return scan_result

    async def _ensure_collection(self) -> AssetCollection | None:
        if not self.include_collection:
            return None
        if self._collection is not None:
            return self._collection

        name = f"Fake Assets ({self.actor.id})"
        db = get_asset_collection_repo()
        existing = await db.get_or_none(name=name)
        if existing is not None:
            self._collection = existing
            return existing

        collection = await db.create(
            name=name,
            description="Synthetic asset collection",
            membership_key_id=get_metadata_id(COLLECTION_MEMBER),
            asset_count=self.total_assets,
        )
        self._collection = collection
        return collection


def _random_datetime(rng: Random, *, days_back: int) -> datetime:
    now = datetime.now(UTC)
    delta_days = rng.randint(0, max(1, days_back))
    return now - timedelta(days=delta_days, seconds=rng.randint(0, 86400))


async def _sleep_batch(rng: Random, delay_ms: float, jitter_ms: float) -> None:
    if delay_ms <= 0 and jitter_ms <= 0:
        return
    jitter = rng.uniform(0, max(0.0, jitter_ms))
    total = max(0.0, (delay_ms + jitter) / 1000.0)
    await asyncio.sleep(total)


def _generate_asset_spec(
    rng: Random,
    actor_id: int,
    index: int,
    *,
    hidden_path_ratio: float = 0.0,
) -> _FakeAssetSpec:
    entry = _pick_weighted(rng, _MIME_TYPES)
    extension = entry["extension"]
    mime_type = entry["mime"]

    base_dir = rng.choice(_ROOT_FOLDERS)
    subdirs = [rng.choice(_SUB_FOLDERS) for _ in range(rng.randint(0, 2))]
    file_name = f"{rng.choice(_FILE_STEMS)}-{index:05d}.{extension}"
    path_parts = ["", "fake", base_dir, *subdirs]

    if hidden_path_ratio and rng.random() < hidden_path_ratio:
        mode = rng.choice(["dot_dir", "dot_file", "tilde_file"])
        if mode == "dot_dir":
            path_parts.append(".hidden")
        elif mode == "dot_file":
            file_name = f".{file_name}"
        else:
            file_name = f"~${file_name}"

    path_parts.append(file_name)
    file_path = "/".join(part for part in path_parts if part)
    size = rng.randint(entry["size_min"], entry["size_max"])
    external_id = f"fake:{actor_id}:{index:06d}"
    canonical_uri = f"fake://{actor_id}/{index}?size={size}"

    return _FakeAssetSpec(
        external_id=external_id,
        canonical_uri=canonical_uri,
        file_name=file_name,
        file_path=file_path,
        file_size=size,
        file_type=mime_type,
        extension=extension,
    )


def _parse_fake_size(uri: str) -> int | None:
    if not uri.startswith("fake://"):
        return None
    parsed = urlparse(uri)
    query = parse_qs(parsed.query)
    size = query.get("size")
    if not size:
        return None
    try:
        return int(size[0])
    except (TypeError, ValueError):
        return None


def _pick_weighted(rng: Random, items: Iterable[dict[str, Any]]) -> dict[str, Any]:
    total = sum(item["weight"] for item in items)
    needle = rng.random() * total
    acc = 0.0
    last = None
    for item in items:
        last = item
        acc += item["weight"]
        if needle <= acc:
            return item
    return last or next(iter(items))


def _fake_id_path(rng: Random, index: int) -> list[str]:
    return [
        f"fake-{index}",
        f"folder-{rng.randint(1, 50)}",
        f"root-{rng.randint(1, 5)}",
    ]


def _shared_with(rng: Random) -> list[str]:
    count = rng.randint(0, 3)
    if count == 0:
        return []
    return rng.sample(_OWNER_POOL, k=count)


def _tags_for_type(rng: Random, mime_type: str) -> list[str]:
    tags = set(rng.sample(_TAG_POOL, k=rng.randint(1, 4)))
    if mime_type.startswith("image/"):
        tags.add("photo")
    if mime_type.startswith("video/"):
        tags.add("video")
    if mime_type.startswith("audio/"):
        tags.add("audio")
    if "pdf" in mime_type:
        tags.add("pdf")
    return sorted(tags)


def _fake_hash(rng: Random, prefix: str) -> str:
    return f"{prefix}-{rng.randrange(16**12):012x}"


def _fake_minhash(rng: Random) -> list[int]:
    return [rng.randint(0, 2**32 - 1) for _ in range(16)]


def _title_from_name(file_name: str) -> str:
    base = file_name.rsplit(".", 1)[0]
    return base.replace("-", " ").title()


def _make_description(rng: Random, spec: _FakeAssetSpec) -> str:
    return f"Synthetic {spec.extension.upper()} asset in {rng.choice(_ROOT_FOLDERS)}"


def _make_comment(rng: Random) -> str:
    return rng.choice(
        [
            "Imported for testing",
            "Generated sample",
            "Synthetic demo asset",
            "QA validation",
        ]
    )


def _make_summary(rng: Random) -> str:
    return rng.choice(
        [
            "Quarterly report draft.",
            "Meeting notes and action items.",
            "Scanned document with annotations.",
            "Creative brief and references.",
        ]
    )


def _download_uri(spec: _FakeAssetSpec) -> str:
    return f"https://assets.example.com/{spec.external_id}/{spec.file_name}"


_ROOT_FOLDERS = [
    "Projects",
    "Photos",
    "Videos",
    "Audio",
    "Archive",
    "Docs",
]

_SUB_FOLDERS = [
    "2021",
    "2022",
    "2023",
    "2024",
    "Client",
    "Personal",
    "Exports",
    "Raw",
]

_FILE_STEMS = [
    "roadmap",
    "contract",
    "invoice",
    "portrait",
    "landscape",
    "notes",
    "meeting",
    "draft",
    "concept",
    "sample",
]

_OWNER_POOL = [
    "alex@example.com",
    "blake@example.com",
    "casey@example.com",
    "dana@example.com",
    "eli@example.com",
]

_AUTHOR_POOL = [
    "Alex Morgan",
    "Blake Lee",
    "Casey Park",
    "Dana Novak",
    "Eli Santos",
]

_TAG_POOL = [
    "work",
    "personal",
    "archive",
    "review",
    "client",
    "draft",
    "export",
    "reference",
]

_MIME_TYPES = [
    {
        "mime": "image/jpeg",
        "extension": "jpg",
        "weight": 0.25,
        "size_min": 50_000,
        "size_max": 6_000_000,
    },
    {
        "mime": "image/png",
        "extension": "png",
        "weight": 0.12,
        "size_min": 80_000,
        "size_max": 8_000_000,
    },
    {
        "mime": "image/heic",
        "extension": "heic",
        "weight": 0.05,
        "size_min": 150_000,
        "size_max": 12_000_000,
    },
    {
        "mime": "application/pdf",
        "extension": "pdf",
        "weight": 0.15,
        "size_min": 40_000,
        "size_max": 15_000_000,
    },
    {
        "mime": "text/plain",
        "extension": "txt",
        "weight": 0.08,
        "size_min": 500,
        "size_max": 250_000,
    },
    {
        "mime": "text/markdown",
        "extension": "md",
        "weight": 0.05,
        "size_min": 500,
        "size_max": 400_000,
    },
    {
        "mime": "video/mp4",
        "extension": "mp4",
        "weight": 0.12,
        "size_min": 3_000_000,
        "size_max": 250_000_000,
    },
    {
        "mime": "audio/mpeg",
        "extension": "mp3",
        "weight": 0.08,
        "size_min": 800_000,
        "size_max": 40_000_000,
    },
    {
        "mime": "application/zip",
        "extension": "zip",
        "weight": 0.05,
        "size_min": 1_000_000,
        "size_max": 80_000_000,
    },
    {
        "mime": "application/octet-stream",
        "extension": "bin",
        "weight": 0.05,
        "size_min": 100_000,
        "size_max": 20_000_000,
    },
]
