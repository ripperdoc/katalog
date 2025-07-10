import asyncio
import datetime
import os.path
from typing import Any, AsyncIterator, Dict

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from clients.base import SourceClient
from models import FileRecord
from utils import parse_google_drive_datetime

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class GoogleDriveClient(SourceClient):
    """
    Client for accessing and listing files in a Google Drive source using a service account.
    """
    def __init__(self, id: str, max_files: int = 500, **kwargs):
        self.id = id
        self.max_files = max_files

        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        self.service = build('drive', 'v3', credentials=creds)

    def get_info(self) -> Dict[str, Any]:
        return {
            "description": "Google Drive client",
            "author": "Katalog Team",
            "version": "0.1"
        }
    
    def get_accessor(self, record: FileRecord) -> Any:

        return None

    async def scan(self) -> AsyncIterator[FileRecord]:
        """
        Asynchronously scan Google Drive and yield FileRecord objects.
        """
        page_token = None
        now = datetime.datetime.utcnow()
        count = 0
        while True:
            try:
                response = self.service.files().list(
                    corpora='user',
                    pageSize=500,
                    fields="nextPageToken, files(id, kind, starred, trashed, description, originalFilename, fileExtension, name, mimeType, size, modifiedTime, createdTime)",
                    pageToken=page_token
                ).execute()
                files = response.get('files', [])
                count += len(files)
                print(f"Scanning Google Drive source, found {count} files...")
                for file in files:
                    try:
                        record = FileRecord(
                            path=file['id'], # Discover path within Google Drive, can be complicated?
                            filename=file.get('originalFilename', file.get('name', '')),
                            source=self.id,
                            size=int(file.get('size', 0)),
                            modified_at=parse_google_drive_datetime(file.get('modifiedTime')),
                            created_at=parse_google_drive_datetime(file.get('createdTime')),
                            scanned_at=now,
                            mime_type=file.get('mimeType'),
                            md5=file.get('md5Checksum', None)
                            # https://developers.google.com/workspace/drive/api/reference/rest/v3/files#File
                            # Other fields: kind, description, name 
                            # thumbnailLink if download thumbnail instead of generating it?
                            # GDrive also has free text labels, although not often used?

                        )
                    except Exception as e:
                        record = FileRecord(
                            path=file.get('id', ''),
                            source=self.id,
                            error_message=str(e),
                            scanned_at=now
                        )
                    yield record
                page_token = response.get('nextPageToken', None)
                if not page_token:
                    break
                if count >= self.max_files:
                    print(f"Reached max files {self.max_files}, stopping scan for source {self.id}.")
                    break
            except HttpError as error:
                yield FileRecord(
                    path='',
                    source=self.id,
                    error_message=f'Google Drive API error: {error}',
                    scanned_at=now
                )
                break
            await asyncio.sleep(0)  # Yield control to event loop
