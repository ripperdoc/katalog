from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from typing import Any, AsyncIterator, Dict, NamedTuple, Optional

import httpx
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from katalog.config import PORT, WORKSPACE
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
    Snapshot,
)
from katalog.sources.base import AssetScanResult, ScanResult, SourcePlugin
from katalog.utils.utils import (
    coerce_int,
    match_paths,
    normalize_glob_patterns,
    parse_google_drive_datetime,
)
from katalog.utils.concurrent_fetcher import ConcurrentSliceFetcher, RequestSpec

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

CLIENT_SECRET_PATH = WORKSPACE / "client_secret.json"
TOKEN_PATH = WORKSPACE / "token.json"
FILE_WEB_VIEW_LINK = define_metadata(
    "file/web_view_link",
    MetadataType.STRING,
    "Web link",
    plugin_id="katalog.sources.googledrive.GoogleDriveClient",
)


class TimeSlice(NamedTuple):
    start: Optional[datetime]
    end: Optional[datetime]
    final: bool


class GoogleDriveClient(SourcePlugin):
    """Client that lists files from Google Drive. Features:
    - Authenticates via OAuth2, by requesting a refresh token that is stored in <WORKSPACE>/token.json
    and using the static application credentials in <WORKSPACE>/credentials.json
    - Reads efficiently from Google Drive API using asyncio fetchers that work concurrently on time-sliced windows, files ordered by modifiedTime desc
    and then regular pagination of 100 within each window (using nextPageToken from the API)
    - Resolves full path metadata per file by recursing parent folder IDs into names. To optimize this, we build a lookup dict that is
    read from and persisted back to `<WORKSPACE>/{provider_id}_folder_cache.json`. If cache doesn't exist, we prepopulate it by reading
    all folders from Drive API. If we discover unknown parents during runtime, we try to look them up with a direct API call,
    and if they are named `Drive` we instead look the name up from the drives/ API endpoint.
    - Supports scanning user drives and shared drives (corpora as a setting per client)
    - Supports exclude/include_path filters to skip (ignore) files that don't have matching name or ID paths
    - Supports incremental scans by finding the last COMPLETE or PARTIAL snapshot and using its start time as a cutoff
    - Supports max_files limit to stop scans early when enough files have been yielded
    """

    def __init__(
        self,
        provider: Provider,
        max_files: int = 0,
        corpora: str = "user",
        allow_incremental: bool = False,
        concurrency: int = 4,
        most_files_within_days: int = 730,
        include_paths: list[str] | str | None = None,
        exclude_paths: list[str] | str | None = None,
        account: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(provider, **kwargs)
        try:
            self.max_files = int(max_files)
        except Exception:
            self.max_files = 0

        # 'user' -> My Drive and Shared with me
        # 'drive:<id>' -> specific shared drive
        # 'allDrives' -> My Drive, Shared with me, and all shared drives
        # 'domain' -> all searchable files
        corpora_value = corpora or "user"
        self.drive_id = None
        if corpora_value.startswith("drive:"):
            self.drive_id = corpora_value.split(":", 1)[1]
            corpora_value = "drive"
        self.corpora = corpora_value
        self.supports_all_drives = self.corpora in {"drive", "allDrives"}
        self.include_paths = normalize_glob_patterns(include_paths)
        self.exclude_paths = normalize_glob_patterns(exclude_paths)
        self.allow_incremental = allow_incremental
        self.concurrency = max(1, int(concurrency))
        self.most_files_within_days = max(1, int(most_files_within_days))
        self.account = account

        # Folder cache state used to reconstruct canonical paths lazily.
        self._folder_cache: Dict[str, Dict[str, Any]] = {}
        self._oauth_state = None
        self._credentials: Credentials | None = None

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Katalog Google Drive client",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def authorize(self, **kwargs) -> str:
        creds = None

        # Run with authorization_response from an Oauth2 callback handler
        if "authorization_response" in kwargs:
            flow = Flow.from_client_secrets_file(
                CLIENT_SECRET_PATH, SCOPES, state=self._oauth_state
            )
            authorization_response = str(kwargs["authorization_response"])
            flow.redirect_uri = f"http://localhost:{PORT}/auth/{self.provider.id}"
            flow.fetch_token(authorization_response=authorization_response)
            creds = flow.credentials
            TOKEN_PATH.write_text(creds.to_json())
            return "authorized"

        # Run without authorization_response, we either refresh or start a new flow
        if TOKEN_PATH.exists():
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                # Write back refreshed token
                TOKEN_PATH.write_text(creds.to_json())
                return "authorized"
            else:
                # Start a new OAuth2 flow to redirect user to authorization URL
                flow = Flow.from_client_secrets_file(CLIENT_SECRET_PATH, SCOPES)
                flow.redirect_uri = f"http://localhost:{PORT}/auth/{self.provider.id}"
                # Read details here https://developers.google.com/identity/protocols/oauth2/web-server#obtainingaccesstokens
                authorization_url, state = flow.authorization_url(
                    access_type="offline",
                    include_granted_scopes="true",
                    login_hint=self.account,
                )
                self._oauth_state = state
                return authorization_url
        else:
            return "authorized"

    def get_accessor(self, asset: Asset) -> Any:
        # TODO: provide streaming accessor for Google Drive file contents.
        return None

    async def build_scan_result(
        self, file: Dict[str, Any], name_paths: list[str], id_paths: list[str]
    ) -> AssetScanResult:
        """Transform a Drive file payload into a AssetScanResult with metadata."""
        file_id = file.get("id", "")
        canonical_uri = f"https://drive.google.com/file/d/{file_id}"

        asset = Asset(
            canonical_id=file_id,
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
        """Asynchronously scan Google Drive and yield AssetScanResults.
        Returns a ScanResult with an async iterator over AssetScanResults. This is created using `build_scan_result` helper that takes
        a Google Drive File object.
        The ScanResult.status should be set to the final status of the scan once complete:
        - OpStatus.COMPLETED: when the scan completed fully
        - OpStatus.PARTIAL: when the scan completed partially due to incremental resumption
        - OpStatus.CANCELED: when the scan was canceled due to reaching max_files limit or if not all produced results were yielded.
        - OpStatus.ERROR when an unrecoverable error occurred
        """
        await self._load_credentials()
        cutoff = None
        if self.allow_incremental:
            last_snapshot = await Snapshot.find_partial_resume_point(
                provider=self.provider
            )
            if last_snapshot:
                cutoff = last_snapshot.started_at

        scan_status = OpStatus.IN_PROGRESS
        ignored = 0
        now = datetime.now(UTC)

        async def iterator() -> AsyncIterator[AssetScanResult]:
            nonlocal scan_status, ignored
            yielded = 0
            try:
                async with httpx.AsyncClient(
                    base_url="https://www.googleapis.com", timeout=5.0
                ) as client:
                    await self._ensure_folder_cache(client)

                    base_params = self._base_list_params()
                    time_slices = list(self._time_slices(now, cutoff))

                    def slice_requests() -> list[RequestSpec]:
                        requests: list[RequestSpec] = []
                        for idx, ts in enumerate(time_slices):
                            params = dict(base_params)
                            params["q"] = self._build_file_query(ts)
                            label = self._describe_slice(idx, ts, len(time_slices))
                            requests.append(
                                RequestSpec(
                                    "GET",
                                    "/drive/v3/files",
                                    params=params,
                                    headers=self._auth_headers(),
                                    log_line=label,
                                )
                            )
                        return requests

                    async def next_page(
                        spec: RequestSpec, response: httpx.Response
                    ) -> RequestSpec | None:
                        payload = response.json()
                        token = payload.get("nextPageToken")
                        if not token:
                            return None
                        params = dict(spec.params or {})
                        params["pageToken"] = token
                        return RequestSpec(
                            spec.method,
                            spec.url,
                            params=params,
                            headers=self._auth_headers(),
                            log_line=spec.log_line,
                        )

                    async with ConcurrentSliceFetcher(
                        client=client,
                        concurrency=self.concurrency,
                        retrying=self._retrying(),
                    ) as fetcher:
                        async for response in fetcher.stream(
                            slices=slice_requests(),
                            next_page=next_page,
                        ):
                            payload = response.json()
                            files = payload.get("files", [])
                            logger.info(
                                f"Fetched {len(files)} files ({yielded} yielded, {ignored} ignored so far)"
                            )
                            for file in files:
                                name_paths, id_paths = await self._resolve_paths(
                                    file, client
                                )
                                if not self._passes_filters(name_paths, id_paths):
                                    ignored += 1
                                    continue
                                result = await self.build_scan_result(
                                    file, name_paths, id_paths
                                )
                                yielded += 1
                                if self.max_files and yielded >= self.max_files:
                                    logger.info(
                                        f"Google Drive scan reached max_files={self.max_files}, stopping early."
                                    )
                                    scan_status = OpStatus.CANCELED
                                    fetcher.cancel()
                                    return
                                yield result
                if scan_status != OpStatus.CANCELED:
                    scan_status = (
                        OpStatus.PARTIAL
                        if cutoff is not None and self.allow_incremental
                        else OpStatus.COMPLETED
                    )
            except Exception as exc:
                scan_status = OpStatus.ERROR
                logger.error(f"Google Drive scan failed: {exc}")
                raise
            finally:
                await self._persist_folder_cache()
                scan_result.status = scan_status
                scan_result.ignored = ignored

        scan_result = ScanResult(
            iterator=iterator(), status=scan_status, ignored=ignored
        )

        return scan_result

    def _retrying(self) -> AsyncRetrying:
        return AsyncRetrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential_jitter(initial=0.5, max=8.0),
            retry=retry_if_exception_type(
                (
                    httpx.TimeoutException,
                    httpx.TransportError,
                    httpx.RemoteProtocolError,
                )
            )
            | retry_if_exception(self._is_retryable_status),
            reraise=True,
        )

    @staticmethod
    def _is_retryable_status(exc: BaseException) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            return status == 429 or status >= 500
        return False

    async def _load_credentials(self) -> Credentials:
        if self._credentials is not None:
            return self._credentials
        if not TOKEN_PATH.exists():
            raise RuntimeError("Google Drive credentials are not valid")
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                TOKEN_PATH.write_text(creds.to_json())
            else:
                raise RuntimeError("Google Drive credentials are not valid")
        self._credentials = creds
        return creds

    def _refresh_credentials_if_needed(self) -> None:
        if self._credentials is None:
            raise RuntimeError("Google Drive credentials are not loaded")
        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(Request())
            TOKEN_PATH.write_text(self._credentials.to_json())

    def _auth_headers(self) -> Dict[str, str]:
        self._refresh_credentials_if_needed()
        assert self._credentials is not None
        return {"Authorization": f"Bearer {self._credentials.token}"}

    def _base_list_params(self) -> Dict[str, Any]:
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

    def _build_file_query(self, time_slice: TimeSlice) -> str:
        clauses = [
            "trashed = false",
            "mimeType != 'application/vnd.google-apps.folder'",
        ]
        time_clause = build_time_query(time_slice.start, time_slice.end)
        if time_clause:
            clauses.append(time_clause)
        return " and ".join(clauses)

    def _time_slices(
        self, end: datetime, cutoff: Optional[datetime]
    ) -> list[TimeSlice]:
        # Create at most `concurrency` slices. First and last slices are open-ended unless a cutoff is set,
        # in which case the oldest slice starts at that cutoff.
        slices: list[TimeSlice] = []
        now = end
        earliest = cutoff or (now - timedelta(days=self.most_files_within_days))
        total_slices = max(1, self.concurrency)

        if total_slices == 1:
            slices.append(TimeSlice(start=cutoff, end=None, final=True))
            return slices

        cut_points: list[datetime] = []
        total_duration = now - earliest
        if total_duration.total_seconds() > 0:
            for idx in range(1, total_slices):
                fraction = idx / total_slices
                cut_points.append(earliest + (total_duration * fraction))

        prev_start: datetime | None = cutoff if cutoff else None
        for boundary in cut_points:
            slices.append(TimeSlice(start=prev_start, end=boundary, final=False))
            prev_start = boundary

        oldest_end: datetime | None = cutoff if cutoff else None
        slices.append(TimeSlice(start=prev_start, end=oldest_end, final=True))
        return slices

    def _describe_slice(self, idx: int, ts: TimeSlice, total: int) -> str:
        start = ts.start.date().isoformat() if ts.start else "begin"
        end = ts.end.date().isoformat() if ts.end else "now"
        return f"slice {idx + 1}/{total} {start} -> {end}"

    def _passes_filters(self, name_paths: list[str], id_paths: list[str]) -> bool:
        if not self.include_paths and not self.exclude_paths:
            return True
        paths = name_paths + id_paths
        return match_paths(
            paths=paths, include=self.include_paths, exclude=self.exclude_paths
        )

    def _folder_cache_path(self) -> Path:
        return WORKSPACE / f"{self.provider.id}_folder_cache.json"

    async def _load_folder_cache(self) -> None:
        path = self._folder_cache_path()
        if path.exists():
            try:
                self._folder_cache = json.loads(path.read_text())
            except Exception as exc:
                logger.warning(f"Failed to load folder cache {path}: {exc}")
                self._folder_cache = {}

    async def _persist_folder_cache(self) -> None:
        path = self._folder_cache_path()
        try:
            path.write_text(json.dumps(self._folder_cache))
        except Exception as exc:
            logger.warning(f"Failed to persist folder cache {path}: {exc}")

    async def _ensure_folder_cache(self, client: httpx.AsyncClient) -> None:
        if self._folder_cache:
            return
        await self._load_folder_cache()
        if self._folder_cache:
            return
        await self._prefetch_folders(client)
        await self._persist_folder_cache()

    async def _prefetch_folders(self, client: httpx.AsyncClient) -> None:
        params = {
            "q": "mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            "fields": "nextPageToken,files(id,name,parents,driveId)",
            "pageSize": 1000,
            "spaces": "drive",
            "supportsAllDrives": self.supports_all_drives,
            "includeItemsFromAllDrives": self.supports_all_drives,
            "corpora": self.corpora,
        }
        if self.drive_id:
            params["driveId"] = self.drive_id
        page_token: Optional[str] = None
        while True:
            if page_token:
                params["pageToken"] = page_token
            response = await client.get(
                "/drive/v3/files", params=params, headers=self._auth_headers()
            )
            response.raise_for_status()
            payload = response.json()
            folders = payload.get("files", [])
            logger.info(f"Prefetched {len(folders)} folders for folder cache")
            for folder in folders:
                folder_id = folder.get("id")
                if not folder_id:
                    continue
                self._folder_cache[folder_id] = {
                    "name": folder.get("name", ""),
                    "parents": folder.get("parents") or [],
                }
            page_token = payload.get("nextPageToken")
            if not page_token:
                break

    async def _get_folder(
        self, folder_id: str, client: httpx.AsyncClient
    ) -> Dict[str, Any]:
        cached = self._folder_cache.get(folder_id)
        if cached:
            return cached
        return await self._fetch_folder_by_id(folder_id, client)

    async def _fetch_folder_by_id(
        self, folder_id: str, client: httpx.AsyncClient
    ) -> Dict[str, Any]:
        response = await client.get(
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
            drive_name = await self._resolve_drive_name(drive_id, client)
            if drive_name:
                name = drive_name
        info = {"name": name, "parents": payload.get("parents") or []}
        self._folder_cache[folder_id] = info
        return info

    async def _resolve_drive_name(
        self, drive_id: str, client: httpx.AsyncClient
    ) -> Optional[str]:
        response = await client.get(
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

    async def _resolve_paths(
        self, file: Dict[str, Any], client: httpx.AsyncClient
    ) -> tuple[list[str], list[str]]:
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
                folder_info = await self._get_folder(current, client)
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


def build_time_query(start: Optional[datetime], end: Optional[datetime]) -> str:
    clauses: list[str] = []
    if start is not None:
        clauses.append(f"modifiedTime >= '{format_dt(start)}'")
    if end is not None:
        clauses.append(f"modifiedTime < '{format_dt(end)}'")
    return " and ".join(clauses)


def format_dt(value: datetime) -> str:
    dt = value if value.tzinfo else value.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    return dt.isoformat().replace("+00:00", "Z")
