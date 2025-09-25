import asyncio
from typing import Any, AsyncIterator, Dict

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
    """
    Client for accessing and listing files in a Google Drive source using a service account.
    """

    PLUGIN_ID = "dev.katalog.client.googledrive"

    def __init__(self, id: str, max_files: int = 500, **kwargs):
        self.id = id
        self.max_files = max_files

        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        token_file = WORKSPACE / "token.json"
        credential_file = WORKSPACE / "credentials.json"
        if token_file.exists():
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credential_file, SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file, "w") as token:
                token.write(creds.to_json())

        self.service = build("drive", "v3", credentials=creds)

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Katalog Google Drive client",
            "author": "Katalog Team",
            "version": "0.1",
        }

    def get_accessor(self, record: FileRecord) -> Any:
        # TODO implement file accessor for Google Drive files
        return None

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Asynchronously scan Google Drive and yield FileRecord objects.
        """
        page_token = None
        count = 0
        while True:
            try:
                response = (
                    self.service.files()
                    .list(
                        corpora="user",
                        pageSize=500,
                        fields="nextPageToken, files(id, kind, starred, trashed, description, originalFilename, fileExtension, name, mimeType, size, modifiedTime, createdTime)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                files = response.get("files", [])
                count += len(files)
                print(f"Scanning Google Drive source, found {count} files...")
                for file in files:
                    try:
                        provider_id = file.get("id", "")
                        size = int(file.get("size")) if file.get("size") else None
                        modified = parse_google_drive_datetime(file.get("modifiedTime"))
                        created = parse_google_drive_datetime(file.get("createdTime"))
                        canonical_uri = f"gdrive://{self.id}/{provider_id}"
                        record = FileRecord(
                            id=provider_id,
                            source_id=self.id,
                            canonical_uri=canonical_uri,
                            # https://developers.google.com/workspace/drive/api/reference/rest/v3/files#File
                            # Other fields: kind, description, name
                            # thumbnailLink if download thumbnail instead of generating it?
                            # GDrive also has free text labels, although not often used?
                        )

                        record.add_metadata(
                            "file/path", self.PLUGIN_ID, file.get("name"), "string"
                        )
                        record.add_metadata(
                            "file/filename",
                            self.PLUGIN_ID,
                            file.get("originalFilename", file.get("name", "")),
                            "string",
                        )

                        if modified:
                            record.add_metadata(
                                "time/modified",
                                self.PLUGIN_ID,
                                modified,
                                "datetime",
                            )

                        if created:
                            record.add_metadata(
                                "time/created",
                                self.PLUGIN_ID,
                                created,
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
                        if size is not None:
                            record.add_metadata(
                                "file/size", self.PLUGIN_ID, size, "int"
                            )

                        starred = file.get("starred")
                        if starred is not None:
                            record.add_metadata(
                                "file/starred",
                                self.PLUGIN_ID,
                                int(bool(starred)),
                                "int",
                            )
                    except Exception as e:
                        provider_id = file.get("id", "error")
                        logger.warning(
                            "Failed to transform Google Drive file %s (%s): %s",
                            file.get("name"),
                            provider_id,
                            e,
                        )
                        continue
                    yield record
                page_token = response.get("nextPageToken", None)
                if not page_token:
                    break
                if count >= self.max_files:
                    print(
                        f"Reached max files {self.max_files}, stopping scan for source {self.id}."
                    )
                    break
            except HttpError as error:
                logger.error(
                    "Google Drive API error for source %s: %s",
                    self.id,
                    error,
                )
                break
            await asyncio.sleep(0)  # Yield control to event loop
