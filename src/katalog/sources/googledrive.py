import asyncio
from datetime import UTC, datetime, timedelta
import json
import secrets
from typing import Any, AsyncIterator, Dict, Optional
from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import RequestQueue

import httpx
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request as GoogleRequest
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from katalog.config import PORT, provider_path
from katalog.metadata import (
    ACCESS_LAST_MODIFYING_USER,
    ACCESS_OWNER,
    ACCESS_SHARED_WITH,
    ACCESS_SHARING_USER,
    FILE_DESCRIPTION,
    FILE_ID_PATH,
    FILE_NAME,
    FILE_PATH,
    FILE_QUOTA_BYTES_USED,
    FILE_SIZE,
    FILE_TYPE,
    FILE_VERSION,
    FLAG_FAVORITE,
    FLAG_SHARED,
    HASH_MD5,
    TIME_ACCESSED_BY_ME,
    TIME_CREATED,
    TIME_MODIFIED,
    TIME_MODIFIED_BY_ME,
    TIME_SHARED_WITH_ME,
    TIME_TRASHED,
    MetadataType,
    define_metadata,
)
from katalog.models import (
    Asset,
    OpStatus,
    Provider,
)
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.utils import (
    TimeSlice,
    coerce_int,
    match_paths,
    normalize_glob_patterns,
    parse_datetime_utc,
    parse_google_drive_datetime,
)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
API_FIELDS = {
    "id",
    "kind",
    "starred",
    "trashed",
    "description",
    "originalFilename",
    "parents",
    "owners",
    "fileExtension",
    "md5Checksum",
    "name",
    "mimeType",
    "size",
    "modifiedTime",
    "createdTime",
    "modifiedByMeTime",
    "viewedByMeTime",
    "sharedWithMeTime",
    "shared",
    "lastModifyingUser",
    "permissions",
    "sharingUser",
    "webViewLink",
    "trashedTime",
    "quotaBytesUsed",
    "version",
}

# Available metadata in Google drive API:
# https://developers.google.com/workspace/drive/api/reference/rest/v3/files#File

QUOTA_QUERIES_PER_SECOND = 200

FILE_WEB_VIEW_LINK = define_metadata(
    "file/web_view_link",
    MetadataType.STRING,
    "Web link",
    plugin_id="katalog.sources.googledrive.GoogleDriveClient",
)


class GoogleDriveClient(SourcePlugin):
    """Client that lists files from Google Drive. Features:
    - Authenticates via OAuth2, by requesting a refresh token that is stored in <provider_path>/token.json
    and using the static application credentials in <provider_path>/client_scret.json
    - Reads efficiently from Google Drive API using asyncio fetchers that work concurrently on time-sliced windows, files ordered by modifiedTime desc
    and then regular pagination of 100 within each window (using nextPageToken from the API)
    - Resolves full path metadata per file by recursing parent folder IDs into names. To optimize this, we build a lookup dict that is
    read from and persisted back to `<provider_path>/folder_cache.json`. If cache doesn't exist, we prepopulate it by reading
    all folders from Drive API. If we discover unknown parents during runtime, we try to look them up with a direct API call,
    and if they are named `Drive` we instead look the name up from the drives/ API endpoint.
    - Supports scanning user drives and shared drives (corpora as a setting per client)
    - Supports exclude/include_path filters to skip (ignore) files that don't have matching name or ID paths
    - Supports restricting scan range with modified_from/modified_to (applied to Drive modifiedTime)
    - Supports max_files limit to stop scans early when enough files have been yielded

    """

    plugin_id = "katalog.sources.googledrive.GoogleDriveClient"
    title = "Google Drive"
    description = "List files from a Google Drive account using OAuth2."

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore", populate_by_name=True)

        max_files: int = Field(
            default=0, ge=0, description="Stop after this many files (0 means no limit)"
        )
        corpora: str = Field(
            default="user",
            description='Drive corpus: "user" (no Shared Drives), "allDrives" (user + Shared Drives), "domain", or "drive"',
        )
        allow_incremental: bool = Field(
            default=False, description="Enable incremental scanning using checkpoints"
        )
        concurrency: int = Field(
            default=10,
            ge=1,
            le=50,
            description="Max concurrent time-slice fetchers",
        )
        include_paths: list[str] | str | None = Field(
            default=None,
            alias="includePaths",
            description="Glob patterns (names or ID paths) to include",
        )
        exclude_paths: list[str] | str | None = Field(
            default=None,
            alias="excludePaths",
            description="Glob patterns to exclude",
        )
        modified_from: datetime | str | None = Field(
            default=None,
            alias="modifiedFrom",
            description="ISO datetime lower bound (Drive modifiedTime)",
        )
        modified_to: datetime | str | None = Field(
            default=None,
            alias="modifiedTo",
            description="ISO datetime upper bound (Drive modifiedTime)",
        )
        account: str | None = Field(
            default=None, description="Login hint (email) for OAuth screen"
        )

        drive_id: str | None = Field(
            default=None, description="Required when corpora=drive"
        )

        @field_validator("include_paths", "exclude_paths", mode="before")
        @classmethod
        def _ensure_list(cls, v):
            if v is None or isinstance(v, list):
                return v
            return [v]

        @field_validator("modified_from", "modified_to", mode="before")
        @classmethod
        def _parse_dt(cls, v):
            if v is None or isinstance(v, datetime):
                return v
            return parse_datetime_utc(v, strict=True)

        @model_validator(mode="after")
        def _normalize(self):
            if (
                self.modified_from
                and self.modified_to
                and self.modified_from >= self.modified_to
            ):
                raise ValueError("modified_from must be before modified_to")
            if self.corpora.startswith("drive:"):
                self.drive_id = self.corpora.split(":", 1)[1] or None
                self.corpora = "drive"
            return self

    config_model = ConfigModel

    def __init__(self, provider: Provider, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(provider, **config)

        self.max_files = cfg.max_files
        self.drive_id = cfg.drive_id
        self.corpora = cfg.corpora
        self.supports_all_drives = self.corpora in {"drive", "allDrives"}
        self.include_paths = normalize_glob_patterns(cfg.include_paths)
        self.exclude_paths = normalize_glob_patterns(cfg.exclude_paths)
        self.allow_incremental = cfg.allow_incremental
        self.concurrency = cfg.concurrency
        self.account = cfg.account

        self.modified_from = parse_datetime_utc(cfg.modified_from, strict=False)
        self.modified_to = parse_datetime_utc(cfg.modified_to, strict=False)
        # Backwards/compat aliases (config may use camelCase).
        self.modifiedFrom = self.modified_from
        self.modifiedTo = self.modified_to

        self.http = httpx.AsyncClient(
            base_url="https://www.googleapis.com", timeout=5.0
        )

        # Folder cache state used to reconstruct canonical paths lazily.
        self._folder_cache: Dict[str, Dict[str, Any]] = {}
        self._oauth_state = None
        self._credentials: Credentials | None = None
        self.token_path = provider_path(self.provider.id) / "token.json"
        self.client_secret_path = provider_path(self.provider.id) / "client_secret.json"
        self.folder_cache_path = provider_path(self.provider.id) / "folder_cache.json"

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Katalog Google Drive client",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def get_accessor(self, asset: Asset) -> Any:
        # TODO: provide streaming accessor for Google Drive file contents.
        return None

    async def close(self) -> None:
        await self.http.aclose()

    async def build_scan_result(
        self, file: Dict[str, Any], name_paths: list[str], id_paths: list[str]
    ) -> AssetScanResult:
        """Transform a Drive file payload into a AssetScanResult with metadata."""
        file_id = file.get("id", "")
        canonical_uri = f"https://drive.google.com/file/d/{file_id}"

        asset = Asset(
            external_id=file_id,
            provider_id=self.provider.id,
            canonical_uri=canonical_uri,
        )

        asset.attach_accessor(self.get_accessor(asset))
        result = AssetScanResult(asset=asset, provider=self.provider)

        result.set_metadata_list(FILE_ID_PATH, id_paths)
        result.set_metadata_list(FILE_PATH, name_paths)

        result.set_metadata(
            FILE_NAME, file.get("originalFilename", file.get("name", ""))
        )

        result.set_metadata(
            TIME_CREATED, parse_google_drive_datetime(file.get("createdTime"))
        )

        result.set_metadata(
            TIME_MODIFIED, parse_google_drive_datetime(file.get("modifiedTime"))
        )

        result.set_metadata(
            TIME_MODIFIED_BY_ME,
            parse_google_drive_datetime(file.get("modifiedByMeTime")),
        )

        result.set_metadata(
            TIME_ACCESSED_BY_ME, parse_google_drive_datetime(file.get("viewedByMeTime"))
        )

        result.set_metadata(
            TIME_SHARED_WITH_ME,
            parse_google_drive_datetime(file.get("viewedByMesharedWithMeTimeTime")),
        )

        result.set_metadata(FILE_TYPE, file.get("mimeType"))

        result.set_metadata(HASH_MD5, file.get("md5Checksum"))

        result.set_metadata(FILE_SIZE, coerce_int(file.get("size")))

        result.set_metadata(FILE_DESCRIPTION, file.get("description"))

        # web_view_link = file.get("webViewLink")
        # if web_view_link:
        #     metadata.append(
        #         make_metadata(self.provider.id, FILE_WEB_VIEW_LINK, web_view_link)
        #     )

        result.set_metadata(FLAG_SHARED, int(bool(file.get("shared"))))

        result.set_metadata(
            ACCESS_LAST_MODIFYING_USER,
            get_user_email(file.get("lastModifyingUser")),
        )

        result.set_metadata(
            ACCESS_SHARING_USER, get_user_email(file.get("sharingUser"))
        )

        result.set_metadata(
            TIME_TRASHED, parse_google_drive_datetime(file.get("trashedTime"))
        )

        result.set_metadata(
            FILE_QUOTA_BYTES_USED, coerce_int(file.get("quotaBytesUsed"))
        )

        result.set_metadata(FILE_VERSION, coerce_int(file.get("version")))

        result.set_metadata(FLAG_FAVORITE, int(bool(file.get("starred"))))

        owners = get_many_user_emails(file.get("owners"))
        result.set_metadata_list(ACCESS_OWNER, owners)

        shared_with = get_many_user_emails(file.get("permissions"))
        result.set_metadata_list(ACCESS_SHARED_WITH, shared_with)

        return result

    async def scan(self) -> ScanResult:
        """Scan Google Drive using HttpCrawler with simple nextPageToken pagination."""
        await self._load_credentials()
        await self._load_folder_cache()

        scan_status = OpStatus.IN_PROGRESS
        time_slice = TimeSlice(start=self.modified_from, end=self.modified_to)

        async def iterator() -> AsyncIterator[AssetScanResult]:
            nonlocal scan_status
            yielded = 0
            ignored = 0
            concurrent = 1
            seen_ids: set[str] = set()

            result_queue: asyncio.Queue[AssetScanResult | BaseException | None] = (
                asyncio.Queue()
            )

            crawler = await make_crawler()

            @crawler.router.default_handler
            async def request_handler(context: HttpCrawlingContext) -> None:
                nonlocal yielded, ignored, scan_status, concurrent
                try:
                    data = await context.http_response.read()
                    payload: dict = json.loads(data)
                    files = payload.get("files", [])
                    counted_files = len(files)

                    # NOTE say this current query returns files that ends with exact same modifiedTime, e.g.
                    # [higher times, 125, 124], 123, 123 and the page after would continue with 123, 123, [122, 121, lower times]
                    # If we decide to split the remaining query into two times slices, but don't want to refetch all results
                    # in this page, we can decide to end the second slice at <123. But then we would miss the files at 123.
                    # If we instead end the slice at <123.001, we will get the 123 files again in the second slice, but may then
                    # yield duplicates. To avoid that, we track seen IDs of files modified at the end boundary of a slice.
                    # A cleaner solution could be to not yield the boundary files in the first place, but that only works if
                    # we know that we later will split the slice rather than paginate it further. So this is a simpler compromise.
                    earliest = None
                    if files:
                        earliest = files[-1].get("modifiedTime")

                    for file in files:
                        if self.max_files and (yielded + ignored) >= self.max_files:
                            scan_status = OpStatus.CANCELED
                            break
                        file_id = file.get("id")
                        if not file_id:
                            ignored += 1
                            continue
                        if file_id in seen_ids:
                            logger.debug(f"Skipping duplicate file ID {file_id}")
                            ignored += 1
                            continue

                        if (
                            earliest
                            and file_id
                            and file.get("modifiedTime") == earliest
                        ):
                            # The file is modified at the end boundary, we might get duplicates
                            # across time slices, so track seen IDs to skip them.
                            seen_ids.add(file_id)

                        name_paths, id_paths = await self._resolve_paths(file)
                        if not self._passes_filters(name_paths, id_paths):
                            ignored += 1
                            continue

                        result = await self.build_scan_result(
                            file, name_paths, id_paths
                        )
                        yielded += 1
                        await result_queue.put(result)

                    ts = TimeSlice.from_dict(
                        context.request.user_data.get("time_slice")
                    )
                    if not ts:
                        raise RuntimeError("Missing time_slice in request user_data")
                    logger.info(
                        f"From slice {ts} got {counted_files} files (total {yielded} yielded, {ignored} ignored)"
                    )
                    next_page = payload.get("nextPageToken")
                    if not next_page or scan_status == OpStatus.CANCELED:
                        # This query slice is done, allow another one to start
                        concurrent = max(0, concurrent - 1)
                        logger.debug(
                            f"Exhausted time slice {ts}, concurrent={concurrent}"
                        )
                        return

                    # If we a next_page, if we can instead split query slice into two for higher concurrency
                    if ts.splittable() and concurrent < self.concurrency:
                        earliest_dt = parse_google_drive_datetime(earliest)
                        if earliest_dt:
                            earliest_dt += timedelta(milliseconds=1)
                        concurrent += 1
                        ts1, ts2 = ts.split(end=earliest_dt)
                        logger.debug(
                            f"Split time slice {ts} into {ts1} and {ts2}, concurrent={concurrent}"
                        )
                        await context.add_requests(
                            [self.make_request("file", time_slice=ts1)]
                        )
                        await context.add_requests(
                            [self.make_request("file", time_slice=ts2)]
                        )
                    # Otherwise, continue paginating within the same query slice
                    else:
                        await context.add_requests(
                            [
                                self.make_request(
                                    "file",
                                    params={"pageToken": next_page},
                                    time_slice=ts,
                                )
                            ]
                        )

                except BaseException as exc:  # noqa: BLE001
                    await result_queue.put(exc)

            async def run_crawler() -> None:
                try:
                    if not self._folder_cache:
                        await self._prefetch_folders()
                    await crawler.run(
                        requests=[self.make_request("file", time_slice=time_slice)]
                    )
                except BaseException as exc:  # noqa: BLE001
                    await result_queue.put(exc)
                finally:
                    # Make best effort to notify the consumer even if it's already cancelled.
                    try:
                        crawler.stop()
                        # Clear crawler storage
                        rq = await crawler.get_request_manager()
                        await rq.drop()
                        await result_queue.put(None)
                    except asyncio.CancelledError:
                        logger.debug(
                            "result_queue.put(None) cancelled; continuing cleanup"
                        )
                    except BaseException as exc:  # noqa: BLE001
                        logger.debug(
                            f"result_queue.put(None) raised {exc!r}; continuing cleanup"
                        )

            producer = asyncio.create_task(run_crawler())
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
                await self._persist_folder_cache()
                if scan_status == OpStatus.IN_PROGRESS:
                    scan_status = OpStatus.COMPLETED
                scan_result.status = scan_status
                scan_result.ignored = ignored

        scan_result = ScanResult(iterator=iterator(), status=scan_status)

        return scan_result

    def authorize(self, **kwargs) -> str:
        creds = None

        # Run with authorization_response from an Oauth2 callback handler
        if "authorization_response" in kwargs:
            flow = Flow.from_client_secrets_file(
                self.client_secret_path, SCOPES, state=self._oauth_state
            )
            authorization_response = str(kwargs["authorization_response"])
            flow.redirect_uri = f"http://localhost:{PORT}/auth/{self.provider.id}"
            flow.fetch_token(authorization_response=authorization_response)
            creds = flow.credentials
            self.token_path.write_text(creds.to_json())
            return "authorized"

        # Run without authorization_response, we either refresh or start a new flow
        if self.token_path.exists():
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        if creds:
            return "authorized"
        else:
            # Start a new OAuth2 flow to redirect user to authorization URL
            flow = Flow.from_client_secrets_file(self.client_secret_path, SCOPES)
            flow.redirect_uri = f"http://localhost:{PORT}/auth/{self.provider.id}"
            # Read details here https://developers.google.com/identity/protocols/oauth2/web-server#obtainingaccesstokens
            authorization_url, state = flow.authorization_url(
                access_type="offline",
                include_granted_scopes="true",
                login_hint=self.account,
            )
            self._oauth_state = state
            return authorization_url

    async def _load_credentials(self) -> Credentials:
        if self._credentials is not None:
            return self._credentials
        if not self.token_path.exists():
            raise RuntimeError("Google Drive credentials are not valid")
        creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(GoogleRequest())
                self.token_path.write_text(creds.to_json())
            else:
                raise RuntimeError("Google Drive credentials are not valid")
        self._credentials = creds
        return creds

    def _refresh_credentials_if_needed(self) -> None:
        if self._credentials is None:
            raise RuntimeError("Google Drive credentials are not loaded")
        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(GoogleRequest())
            self.token_path.write_text(self._credentials.to_json())

    def _auth_headers(self) -> Dict[str, str]:
        self._refresh_credentials_if_needed()
        assert self._credentials is not None
        return {"Authorization": f"Bearer {self._credentials.token}"}

    def _base_file_list_params(self) -> Dict[str, Any]:
        fields = f"nextPageToken,files({','.join(sorted(API_FIELDS))})"
        params: Dict[str, Any] = {
            "pageSize": 100,
            "orderBy": "modifiedTime desc",
            "fields": fields,
            "spaces": "drive",
            "corpora": self.corpora,
            "supportsAllDrives": self.supports_all_drives,
            "includeItemsFromAllDrives": self.supports_all_drives,
        }
        if self.drive_id:
            params["driveId"] = self.drive_id
        return params

    def _base_folder_list_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "pageSize": 1000,
            "q": "mimeType = 'application/vnd.google-apps.folder'",
            "fields": "nextPageToken,files(id,name,parents,modifiedTime)",
            "spaces": "drive",
            "corpora": self.corpora,
            "supportsAllDrives": self.supports_all_drives,
            "includeItemsFromAllDrives": self.supports_all_drives,
        }
        if self.drive_id:
            params["driveId"] = self.drive_id
        return params

    def make_request(
        self,
        type: str,
        time_slice: TimeSlice,
        label: str | None = None,
        params: dict | None = None,
        headers: dict | None = None,
        **kwargs,
    ):
        user_data = kwargs or {}

        if type == "file":
            base_params = self._base_file_list_params()
        elif type == "folder":
            base_params = self._base_folder_list_params()
        else:
            raise ValueError(f"Unknown request type: {type}")
        params = {**base_params, **(params or {})}

        if time_slice:
            time_query = build_time_query(time_slice)
            if "q" in params:
                params["q"] = f"({params['q']}) and ({time_query})"
            else:
                params["q"] = time_query
            user_data["time_slice"] = time_slice.to_dict()

        url = str(
            httpx.URL(
                "https://www.googleapis.com/drive/v3/files",
                params=params,
            )
        )
        base_headers = self._auth_headers()
        base_headers.update(headers or {})
        try:
            req = Request.from_url(
                url=url,
                method="GET",
                headers=base_headers,
                user_data=user_data,
                label=label,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create request for URL {url}: {exc}"
            ) from exc
        return req

    def _passes_filters(self, name_paths: list[str], id_paths: list[str]) -> bool:
        if not self.include_paths and not self.exclude_paths:
            return True
        paths = name_paths + id_paths
        return match_paths(
            paths=paths, include=self.include_paths, exclude=self.exclude_paths
        )

    async def _load_folder_cache(self) -> None:
        if self._folder_cache:
            return
        path = self.folder_cache_path
        if path.exists():
            try:
                self._folder_cache = json.loads(path.read_text())
            except Exception as exc:
                logger.warning(f"Failed to load folder cache {path}: {exc}")
                self._folder_cache = {}

    async def _persist_folder_cache(self) -> None:
        path = self.folder_cache_path
        try:
            path.write_text(json.dumps(self._folder_cache))
        except Exception as exc:
            logger.warning(f"Failed to persist folder cache {path}: {exc}")

    async def _prefetch_folders(self) -> None:
        crawler = await make_crawler()
        concurrent = 1
        total_folders = 0

        @crawler.router.default_handler
        async def folder_request_handler(context: HttpCrawlingContext) -> None:
            nonlocal concurrent, total_folders
            data = await context.http_response.read()
            payload: dict = json.loads(data)
            files = payload.get("files", [])
            earliest = None
            if files:
                earliest = files[-1].get("modifiedTime")
            total_folders += len(files)
            for file in files:
                folder_id = file.get("id")
                if folder_id:
                    self._folder_cache[folder_id] = {
                        "name": file.get("name", ""),
                        "parents": file.get("parents") or [],
                    }

            logger.info(
                f"Prefetched {len(files)} folders into cache (total fetched {total_folders})"
            )
            ts = TimeSlice.from_dict(context.request.user_data.get("time_slice"))
            if not ts:
                raise RuntimeError("Missing time_slice in request user_data")
            next_page = payload.get("nextPageToken")
            if not next_page:
                # This query slice is done, allow another one to start
                concurrent = max(0, concurrent - 1)
                logger.debug(f"Exhausted time slice {ts}, concurrent={concurrent}")
                return

            # If we a next_page, if we can instead split query slice into two for higher concurrency
            if ts.splittable() and concurrent < self.concurrency:
                earliest_dt = parse_google_drive_datetime(earliest)
                if earliest_dt:
                    earliest_dt += timedelta(milliseconds=1)
                concurrent += 1
                ts1, ts2 = ts.split(end=earliest_dt)
                logger.debug(
                    f"Split time slice {ts} into {ts1} and {ts2}, concurrent={concurrent}"
                )
                await context.add_requests(
                    [self.make_request("folder", time_slice=ts1)]
                )
                await context.add_requests(
                    [self.make_request("folder", time_slice=ts2)]
                )
            # Otherwise, continue paginating within the same query slice
            else:
                await context.add_requests(
                    [
                        self.make_request(
                            "folder", params={"pageToken": next_page}, time_slice=ts
                        )
                    ]
                )

        await crawler.run(
            [
                self.make_request(
                    "folder",
                    time_slice=TimeSlice(start=None, end=None),
                    params={
                        "q": "mimeType = 'application/vnd.google-apps.folder'",
                        "fields": "nextPageToken,files(id,name,parents,modifiedTime)",
                        "pageSize": 1000,
                    },
                )
            ]
        )

    async def _get_folder(self, folder_id: str) -> Dict[str, Any]:
        cached = self._folder_cache.get(folder_id)
        if cached:
            return cached
        return await self._fetch_folder_by_id(folder_id)

    async def _fetch_folder_by_id(self, folder_id: str) -> Dict[str, Any]:
        response = await self.http.get(
            f"/drive/v3/files/{folder_id}",
            params={
                "fields": "id,name,parents,driveId",
                "supportsAllDrives": self.supports_all_drives,
                "includeItemsFromAllDrives": self.supports_all_drives,
            },
            headers=self._auth_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        name = payload.get("name", "")
        logger.info(f"Fetched folder {folder_id} with name '{name}'")
        drive_id = payload.get("driveId")
        if name == "Drive" and drive_id:
            drive_name = await self._resolve_drive_name(drive_id)
            if drive_name:
                name = drive_name
        info = {"name": name, "parents": payload.get("parents") or []}
        self._folder_cache[folder_id] = info
        return info

    async def _resolve_drive_name(self, drive_id: str) -> Optional[str]:
        response = await self.http.get(
            f"/drive/v3/drives/{drive_id}",
            params={"fields": "name"},
            headers=self._auth_headers(),
        )
        logger.info(
            f"Resolving drive name for drive ID {drive_id}, status {response.status_code}"
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        return payload.get("name")

    async def _resolve_paths(self, file: Dict[str, Any]) -> tuple[list[str], list[str]]:
        file_name = file.get("name", file.get("originalFilename", ""))
        file_id = file.get("id", "")
        parents = file.get("parents") or []
        if not parents:
            return [file_name], [file_id]

        name_paths: list[str] = []
        id_paths: list[str] = []
        for parent_id in parents:
            names: list[str] = []
            ids: list[str] = []
            current = parent_id
            seen: set[str] = set()
            while current and current not in seen:
                seen.add(current)
                folder_info = await self._get_folder(current)
                names.append(folder_info.get("name", ""))
                ids.append(current)
                parent_list = folder_info.get("parents") or []
                current = parent_list[0] if parent_list else None
            names.reverse()
            ids.reverse()
            name_paths.append("/".join(names + [file_name]))
            id_paths.append("/".join(ids + [file_id]))
        return name_paths, id_paths


def get_user_email(user_like_object: Any) -> Optional[str]:
    """Extract the user's email address from Google user or permission info payload."""
    if not user_like_object:
        return None
    email = user_like_object.get("emailAddress")
    if isinstance(email, str):
        return email
    return None


def get_many_user_emails(user_like_objects: Any) -> set[str]:
    """Extract multiple user email addresses from a list of Google user or permission info payloads."""
    emails: set[str] = set()
    if not isinstance(user_like_objects, list):
        return emails
    for user_like_object in user_like_objects:
        email = get_user_email(user_like_object)
        if email:
            emails.add(email)
    return emails


def build_time_query(ts: TimeSlice) -> str:
    clauses: list[str] = []
    if ts.start is not None:
        clauses.append(f"modifiedTime >= '{format_dt(ts.start)}'")
    if ts.end is not None:
        clauses.append(f"modifiedTime < '{format_dt(ts.end)}'")
    return " and ".join(clauses)


def format_dt(value: datetime) -> str:
    dt = value if value.tzinfo else value.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")


async def make_crawler():
    # Use a fresh storage client and request queue per scan to avoid reusing state across runs.
    storage_client = MemoryStorageClient()
    request_queue = await RequestQueue.open(
        alias=f"googledrive-{secrets.token_hex(4)}",
        storage_client=storage_client,
    )
    crawler = HttpCrawler(storage_client=storage_client, request_manager=request_queue)
    return crawler
