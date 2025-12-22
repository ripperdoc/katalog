import asyncio
import pickle
from typing import Any, Dict, List, Optional, cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

from katalog.config import WORKSPACE
from katalog.models import (
    Asset,
    OpStatus,
    Metadata,
    MetadataType,
    Provider,
    Snapshot,
)
from katalog.metadata import (
    ACCESS_OWNER,
    FLAG_SHARED,
    ACCESS_SHARED_WITH,
    ACCESS_SHARING_USER,
    FILE_DESCRIPTION,
    FILE_ID_PATH,
    ACCESS_LAST_MODIFYING_USER,
    FILE_NAME,
    FILE_PATH,
    FILE_QUOTA_BYTES_USED,
    FILE_SIZE,
    FILE_VERSION,
    HASH_MD5,
    FILE_TYPE,
    FLAG_FAVORITE,
    TIME_CREATED,
    TIME_MODIFIED,
    TIME_MODIFIED_BY_ME,
    TIME_SHARED_WITH_ME,
    TIME_TRASHED,
    TIME_ACCESSED_BY_ME,
    define_metadata,
)
from katalog.sources.base import (
    AssetRecordResult,
    ScanResult,
    SourcePlugin,
)
from katalog.utils.utils import fqn, parse_google_drive_datetime


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


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        value = stripped
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_modified(metadata: list[Metadata]) -> Optional[Any]:
    for entry in metadata:
        if entry.key == TIME_MODIFIED:
            return entry.value  # datetime
    return None


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
_API_FIELDS = {
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


class GoogleDriveClient(SourcePlugin):
    """Client that lists files from Google Drive."""

    def __init__(self, provider: Provider, max_files: int = 500, **kwargs: Any) -> None:
        super().__init__(provider, **kwargs)
        self.max_files = max_files

        creds: Optional[Credentials] = None
        workspace_token = WORKSPACE / "token.json"
        workspace_credentials = WORKSPACE / "credentials.json"
        if workspace_token.exists():
            creds = Credentials.from_authorized_user_file(workspace_token, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    workspace_credentials, SCOPES
                )
                creds = cast(Credentials, flow.run_local_server(port=0))
            if creds:
                with workspace_token.open("w") as token_file:
                    token_file.write(creds.to_json())

        self.service = build("drive", "v3", credentials=creds)

        # Folder cache state used to reconstruct canonical paths lazily.
        self._folder_cache: Dict[str, Dict[str, Any]] = {}
        self._folder_page_token: Optional[str] = None
        self._folders_exhausted = False

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Katalog Google Drive client",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def get_accessor(self, asset: Asset) -> Any:
        # TODO: provide streaming accessor for Google Drive file contents.
        return None

    async def scan(self, *, since_snapshot: Snapshot | None = None) -> ScanResult:
        """Asynchronously scan Google Drive and yield AssetRecord objects."""

        limit_reached = False
        cutoff_reached = False

        async def inner():
            nonlocal limit_reached, cutoff_reached
            self._prime_folder_cache()
            page_token: Optional[str] = None
            count = 0
            cutoff = None
            if since_snapshot:
                cutoff = since_snapshot.completed_at or since_snapshot.started_at
                if cutoff:
                    logger.info(
                        f"Incremental scan for source {self.provider.id} — cutoff {cutoff}"
                    )
            try:
                while True:
                    response = self._fetch_files_page(page_token)
                    if response is None:
                        break

                    files = response.get("files", [])
                    logger.info(f"Fetched {len(files)} files from Google Drive API")
                    for file in files:
                        if self.max_files and count >= self.max_files:
                            limit_reached = True
                            logger.info(
                                f"Reached max files {self.max_files} — stopping scan for source {self.provider.id}"
                            )
                            break

                        result = self._build_result(file)
                        if result.asset and result.metadata:
                            if cutoff:
                                modified_dt = _extract_modified(result.metadata)
                                if modified_dt and modified_dt < cutoff:
                                    cutoff_reached = True
                                    logger.info(
                                        f"Stopping scan for source {self.provider.id} at cutoff {cutoff} (latest {modified_dt})"
                                    )
                                    break
                            yield result
                            count += 1
                        else:
                            logger.warning(
                                f"Skipping invalid file record in source {self.provider.id}: {file}"
                            )

                    logger.info(
                        f"Scanning Google Drive source {self.provider.id} — processed {count} files so far"
                    )

                    if cutoff_reached or limit_reached:
                        break

                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break
                    await asyncio.sleep(0)
            finally:
                if limit_reached:
                    scan_result.status = OpStatus.CANCELED
                elif cutoff_reached:
                    scan_result.status = OpStatus.PARTIAL
                else:
                    scan_result.status = OpStatus.COMPLETED
                self._persist_folder_cache()

        scan_result = ScanResult(iterator=inner())
        return scan_result

    def _build_result(self, file: Dict[str, Any]) -> AssetRecordResult:
        """Transform a Drive file payload into a AssetRecord and metadata, guarding errors."""
        file_id = file.get("id", "")
        canonical_uri = f"https://drive.google.com/file/d/{file_id}"

        asset = Asset(
            canonical_id=file_id,
            provider_id=self.provider.id,
            canonical_uri=canonical_uri,
        )
        asset.attach_accessor(self.get_accessor(asset))
        result = AssetRecordResult(asset=asset, provider=self.provider)

        name_paths, id_paths = self._resolve_paths(file)
        result.add_metadata_set(FILE_ID_PATH, id_paths)
        result.add_metadata_set(FILE_PATH, name_paths)

        result.add_metadata(
            FILE_NAME, file.get("originalFilename", file.get("name", ""))
        )

        result.add_metadata(
            TIME_CREATED, parse_google_drive_datetime(file.get("createdTime"))
        )

        result.add_metadata(
            TIME_MODIFIED, parse_google_drive_datetime(file.get("modifiedTime"))
        )

        result.add_metadata(
            TIME_MODIFIED_BY_ME,
            parse_google_drive_datetime(file.get("modifiedByMeTime")),
        )

        result.add_metadata(
            TIME_ACCESSED_BY_ME, parse_google_drive_datetime(file.get("viewedByMeTime"))
        )

        result.add_metadata(
            TIME_SHARED_WITH_ME,
            parse_google_drive_datetime(file.get("viewedByMesharedWithMeTimeTime")),
        )

        result.add_metadata(FILE_TYPE, file.get("mimeType"))

        result.add_metadata(HASH_MD5, file.get("md5Checksum"))

        result.add_metadata(FILE_SIZE, _coerce_int(file.get("size")))

        result.add_metadata(FILE_DESCRIPTION, file.get("description"))

        # web_view_link = file.get("webViewLink")
        # if web_view_link:
        #     metadata.append(
        #         make_metadata(self.provider.id, FILE_WEB_VIEW_LINK, web_view_link)
        #     )

        result.add_metadata(FLAG_SHARED, int(bool(file.get("shared"))))

        result.add_metadata(
            ACCESS_LAST_MODIFYING_USER,
            get_user_email(file.get("lastModifyingUser")),
        )

        result.add_metadata(ACCESS_SHARING_USER, file.get("sharingUser"))

        result.add_metadata(
            TIME_TRASHED, parse_google_drive_datetime(file.get("trashedTime"))
        )

        result.add_metadata(
            FILE_QUOTA_BYTES_USED, _coerce_int(file.get("quotaBytesUsed"))
        )

        result.add_metadata(FILE_VERSION, _coerce_int(file.get("version")))

        result.add_metadata(FLAG_FAVORITE, int(bool(file.get("starred"))))

        owners = get_many_user_emails(file.get("owners"))
        result.add_metadata_set(ACCESS_OWNER, owners)

        shared_with = get_many_user_emails(file.get("permissions"))
        result.add_metadata_set(ACCESS_SHARED_WITH, shared_with)

        return result

    def _fetch_files_page(
        self,
        page_token: Optional[str],
        page_size: int = 500,
    ) -> Optional[Dict[str, Any]]:
        try:
            response = (
                self.service.files()
                .list(
                    corpora="user",
                    pageSize=page_size,
                    fields=(
                        "nextPageToken, files(" + ", ".join(sorted(_API_FIELDS)) + ")"
                    ),
                    pageToken=page_token,
                    orderBy="modifiedTime desc",
                )
                .execute()
            )
            return response
        except HttpError as error:
            logger.error(
                f"Google Drive API error for source {self.provider.id}: {error}"
            )
            return None

    def _resolve_paths(self, file: Dict[str, Any]) -> tuple[list[str], list[str]]:
        """Return all name-based and ID-based paths for a Drive file."""
        file_id = file.get("id", "")
        leaf_name = self._sanitize_component(file.get("name") or file_id)
        leaf_id = file_id.strip()
        parent_paths = self._collect_parent_paths(file.get("parents") or [], set())
        if not parent_paths:
            parent_paths = [([], [])]

        name_paths: list[str] = []
        id_paths: list[str] = []
        name_seen: set[str] = set()
        id_seen: set[str] = set()
        for name_chain, id_chain in parent_paths:
            name_components = name_chain + [leaf_name]
            id_components = id_chain + [leaf_id]
            name_path = "/".join(filter(None, name_components))
            id_path = "/".join(filter(None, id_components))
            if name_path and name_path not in name_seen:
                name_paths.append(name_path)
                name_seen.add(name_path)
            if id_path and id_path not in id_seen:
                id_paths.append(id_path)
                id_seen.add(id_path)
        return name_paths, id_paths

    def _collect_parent_paths(
        self, parent_ids: List[str], visited: set[str]
    ) -> list[tuple[list[str], list[str]]]:
        if not parent_ids:
            return [([], [])]
        results: list[tuple[list[str], list[str]]] = []
        for parent_id in parent_ids:
            sanitized_id = parent_id.strip()
            branch_visited = visited | {parent_id}
            if parent_id in visited:
                fallback_name = self._sanitize_component(parent_id)
                results.append(([fallback_name], [sanitized_id]))
                continue
            folder = self._folder_cache.get(parent_id)
            if not folder:
                folder = self._ensure_folder_loaded(parent_id)
            if not folder:
                fallback_name = self._sanitize_component(parent_id)
                results.append(([fallback_name], [sanitized_id]))
                continue
            folder_name = self._sanitize_component(folder.get("name") or parent_id)
            folder_parents = folder.get("parents") or []
            subpaths = self._collect_parent_paths(folder_parents, branch_visited)
            for name_chain, id_chain in subpaths:
                results.append((name_chain + [folder_name], id_chain + [sanitized_id]))
        return results

    def _sanitize_component(self, value: Optional[str]) -> str:
        """Trim whitespace and encode literal slashes in a path component."""
        cleaned = (value or "").strip()
        return cleaned.replace("/", "%2F")

    def _prime_folder_cache(self) -> None:
        cache, page_token, exhausted = self._load_folder_cache_from_disk()
        self._folder_cache = cache
        self._folder_page_token = page_token
        self._folders_exhausted = exhausted
        logger.info(
            f"Primed folder cache with {len(self._folder_cache)} entries (exhausted? {self._folders_exhausted})"
        )

    def _cache_file_path(self):
        return WORKSPACE / f"{self.provider.id}_folder_cache.pkl"

    def _load_folder_cache_from_disk(
        self,
    ) -> tuple[Dict[str, Dict[str, Any]], Optional[str], bool]:
        path = self._cache_file_path()
        if not path.exists():
            return {}, None, False
        try:
            with path.open("rb") as fh:
                data = pickle.load(fh)
            folders = data.get("folders", {})
            page_token = data.get("page_token")
            exhausted = bool(data.get("exhausted", False))
            return folders, page_token, exhausted
        except Exception as exc:  # pragma: no cover - cache corruption is rare
            logger.warning(f"Failed to load folder cache from {path}: {exc}")
            return {}, None, False

    def _persist_folder_cache(self) -> None:
        path = self._cache_file_path()
        payload = {
            "folders": self._folder_cache,
            "page_token": None if self._folders_exhausted else self._folder_page_token,
            "exhausted": self._folders_exhausted,
        }
        try:
            with path.open("wb") as fh:
                pickle.dump(payload, fh)
        except Exception as exc:  # pragma: no cover - disk errors rare
            logger.warning(f"Failed to persist folder cache to {path}: {exc}")

    def _fetch_next_folder_page(self, page_size: int = 500) -> None:
        if self._folders_exhausted:
            return
        logger.info(f"Google Drive API: listing next {page_size} folders")
        response = (
            self.service.files()
            .list(
                q="mimeType='application/vnd.google-apps.folder'",
                fields="nextPageToken, files(id, name, parents)",
                corpora="user",
                pageSize=page_size,
                pageToken=self._folder_page_token,
            )
            .execute()
        )
        logger.info(f"Fetched {len(response)} folders for cache")
        for folder in response.get("files", []):
            self._folder_cache[folder["id"]] = {
                "name": folder.get("name") or folder["id"],
                "parents": folder.get("parents", []),
            }
        self._folder_page_token = response.get("nextPageToken")
        if not self._folder_page_token:
            self._folders_exhausted = True
        self._persist_folder_cache()

    def _ensure_folder_loaded(self, folder_id: str) -> Optional[Dict[str, Any]]:
        if not folder_id:
            return None
        cached = self._folder_cache.get(folder_id)
        if cached:
            return cached
        while not self._folders_exhausted:
            self._fetch_next_folder_page()
            cached = self._folder_cache.get(folder_id)
            if cached:
                return cached
        return None


FILE_WEB_VIEW_LINK = define_metadata(
    "file/web_view_link",
    MetadataType.STRING,
    "Web link",
    plugin_id=fqn(GoogleDriveClient),
)
