import asyncio
import pickle
from typing import Any, AsyncIterator, Dict, List, Optional, cast

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

from katalog.clients.base import SourceClient
from katalog.config import WORKSPACE
from katalog.models import FileRecord
from katalog.utils.utils import parse_google_drive_datetime

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class GoogleDriveClient(SourceClient):
    """Client that lists files from Google Drive."""

    PLUGIN_ID = "dev.katalog.client.googledrive"

    def __init__(self, id: str, max_files: int = 500, **_: Any) -> None:
        self.id = id
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

    def get_accessor(self, record: FileRecord) -> Any:
        # TODO: provide streaming accessor for Google Drive file contents.
        return None

    async def scan(self) -> AsyncIterator[FileRecord]:
        """Asynchronously scan Google Drive and yield FileRecord objects."""
        self._prime_folder_cache()
        page_token: Optional[str] = None
        count = 0
        try:
            while True:
                try:
                    response = (
                        self.service.files()
                        .list(
                            corpora="user",
                            pageSize=500,
                            fields="""nextPageToken, files(id, kind, starred, trashed, description, originalFilename, parents, owners, fileExtension, md5Checksum, name, mimeType, size, modifiedTime, createdTime)""",
                            pageToken=page_token,
                            orderBy="modifiedTime desc",
                        )
                        .execute()
                    )
                    files = response.get("files", [])
                    count += len(files)
                    logger.info(
                        "Scanning Google Drive source {} — processed {} files so far",
                        self.id,
                        count,
                    )
                    for file in files:
                        try:
                            file_id = file.get("id", "")
                            canonical_uri = f"https://drive.google.com/file/d/{file_id}"

                            record = FileRecord(
                                id=file_id,
                                source_id=self.id,
                                canonical_uri=canonical_uri,
                            )

                            name_paths, id_paths = self._resolve_paths(file)
                            for path in name_paths:
                                record.add_metadata(
                                    "file/path", self.PLUGIN_ID, path, "string"
                                )
                            for path in id_paths:
                                record.add_metadata(
                                    "file/path_ids",
                                    self.PLUGIN_ID,
                                    path,
                                    "string",
                                )
                            record.add_metadata(
                                "file/filename",
                                self.PLUGIN_ID,
                                file.get("originalFilename", file.get("name", "")),
                                "string",
                            )

                            created = parse_google_drive_datetime(
                                file.get("createdTime")
                            )
                            if created:
                                record.add_metadata(
                                    "time/created",
                                    self.PLUGIN_ID,
                                    created,
                                    "datetime",
                                )

                            modified = parse_google_drive_datetime(
                                file.get("modifiedTime")
                            )
                            if modified:
                                record.add_metadata(
                                    "time/modified",
                                    self.PLUGIN_ID,
                                    modified,
                                    "datetime",
                                )

                            if file.get("mimeType"):
                                record.add_metadata(
                                    "mime/type",
                                    self.PLUGIN_ID,
                                    file.get("mimeType"),
                                    "string",
                                )
                            if file.get("md5Checksum"):
                                record.add_metadata(
                                    "hash/md5",
                                    self.PLUGIN_ID,
                                    file.get("md5Checksum"),
                                    "string",
                                )

                            size = int(file.get("size")) if file.get("size") else None
                            if size is not None:
                                record.add_metadata(
                                    "file/size", self.PLUGIN_ID, size, "int"
                                )

                            owners = file.get("owners") or []
                            if owners:
                                for owner in owners:
                                    record.add_metadata(
                                        "file/owner",
                                        self.PLUGIN_ID,
                                        owner["emailAddress"],
                                        "json",
                                    )
                            starred = file.get("starred")
                            if starred is not None:
                                record.add_metadata(
                                    "file/starred",
                                    self.PLUGIN_ID,
                                    int(bool(starred)),
                                    "int",
                                )
                        except Exception as exc:  # pragma: no cover - defensive
                            file_id = file.get("id", "error")
                            logger.warning(
                                "Failed to transform Google Drive file %s (%s): %s",
                                file.get("name"),
                                file_id,
                                exc,
                            )
                            continue
                        yield record
                    page_token = response.get("nextPageToken")
                    if not page_token:
                        break
                    if count >= self.max_files:
                        logger.info(
                            "Reached max files {} — stopping scan for source {}",
                            self.max_files,
                            self.id,
                        )
                        break
                except HttpError as error:
                    logger.error(
                        "Google Drive API error for source %s: %s",
                        self.id,
                        error,
                    )
                    break
                await asyncio.sleep(0)
        finally:
            self._persist_folder_cache()

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
            "Primed folder cache with {} entries (exhausted? {})",
            len(self._folder_cache),
            self._folders_exhausted,
        )

    def _cache_file_path(self):
        return WORKSPACE / f"{self.id}_folder_cache.pkl"

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
            logger.warning("Failed to load folder cache from {}: {}", path, exc)
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
            logger.warning("Failed to persist folder cache to {}: {}", path, exc)

    def _fetch_next_folder_page(self) -> None:
        if self._folders_exhausted:
            return
        response = (
            self.service.files()
            .list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="nextPageToken, files(id, name, parents)",
                corpora="user",
                pageSize=500,
                pageToken=self._folder_page_token,
            )
            .execute()
        )
        logger.info("Fetched {} folders for cache", len(response))
        for folder in response.get("files", []):
            self._folder_cache[folder["id"]] = {
                "name": folder.get("name") or folder["id"],
                "parents": folder.get("parents", []),
            }
        self._folder_page_token = response.get("nextPageToken")
        if not self._folder_page_token:
            self._folders_exhausted = True

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
