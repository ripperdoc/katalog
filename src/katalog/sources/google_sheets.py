from __future__ import annotations

import os
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import quote

import httpx
from loguru import logger
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from pydantic import Field, field_validator

from katalog.config import PORT, actor_path
from katalog.models import Actor
from katalog.sources.base import ScanResult
from katalog.sources.tabular import TabularRawRow, TabularSource, TabularSourceConfig

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _allow_insecure_local_oauth_transport() -> None:
    # Local development callback is HTTP on localhost.
    os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")
    # Google may return previously granted superset scopes.
    os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")


class GoogleSheetsSourceConfig(TabularSourceConfig):
    spreadsheet_id: str = Field(
        ...,
        description="Google Sheets spreadsheet ID.",
    )
    worksheet: str | None = Field(
        default=None,
        description="Worksheet (tab) name. Used when range_a1 is not provided.",
    )
    range_a1: str | None = Field(
        default=None,
        description=(
            "Optional A1 range, e.g. 'Products!A:Z'. "
            "When omitted, reads all columns from the selected worksheet."
        ),
    )
    account: str | None = Field(
        default=None,
        description="Login hint (email) shown on OAuth screen.",
    )
    client_secret_path: str | None = Field(
        default=None,
        description=(
            "Optional path to OAuth client secret JSON. "
            "Defaults to actor_path/<actor_id>/client_secret.json."
        ),
    )

    @field_validator("spreadsheet_id", mode="before")
    @classmethod
    def _normalize_spreadsheet_id(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("spreadsheet_id is required")
        return text

    @field_validator("worksheet", "range_a1", "account", "client_secret_path", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class GoogleSheetsSource(TabularSource):
    plugin_id = "katalog.sources.google_sheets.GoogleSheetsSource"
    title = "Google Sheets"
    description = "Read tabular rows from Google Sheets using OAuth2."
    config_model = GoogleSheetsSourceConfig

    def __init__(self, actor: Actor, **config: Any) -> None:
        cfg = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)
        self.spreadsheet_id = cfg.spreadsheet_id
        self.worksheet = cfg.worksheet
        self.range_a1 = cfg.range_a1
        self.account = cfg.account
        self.http = httpx.AsyncClient(
            base_url="https://sheets.googleapis.com",
            timeout=30.0,
        )
        self._credentials: Credentials | None = None
        self._oauth_state: str | None = None

        actor_id = self.actor.id
        if actor_id is None:
            raise ValueError("GoogleSheetsSource actor is missing id")
        self.token_path = actor_path(actor_id) / "token.json"
        if cfg.client_secret_path:
            self.client_secret_path = Path(cfg.client_secret_path).expanduser()
        else:
            self.client_secret_path = actor_path(actor_id) / "client_secret.json"

    def get_info(self) -> dict[str, Any]:
        return {
            "description": "Google Sheets source",
            "version": "0.1",
        }

    async def close(self) -> None:
        await self.http.aclose()

    def source_uri(self) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"

    def row_path_value(self, row_number: int) -> str | None:
        # Prefer worksheet-qualified A1 row notation for clarity in UI.
        if self.range_a1 and "!" in self.range_a1:
            sheet_selector = self.range_a1.split("!", 1)[0]
            return f"{sheet_selector}!{row_number}:{row_number}"
        if self.worksheet:
            escaped = self.worksheet.replace("'", "''")
            return f"'{escaped}'!{row_number}:{row_number}"
        return f"{row_number}:{row_number}"

    def source_debug_location(self) -> str:
        return (
            f"{self.source_uri()} "
            f"(worksheet={self.worksheet or '<default>'}, range={self._effective_range()})"
        )

    def can_scan_uri(self, uri: str) -> bool:
        return uri.startswith("https://docs.google.com/spreadsheets/")

    async def is_ready(self) -> tuple[bool, str | None]:
        if not self.token_path.exists():
            return (
                False,
                f"Missing OAuth token file: {self.token_path}. Authorize this actor first.",
            )
        try:
            await self._load_credentials()
        except Exception as exc:  # noqa: BLE001
            return False, f"Google Sheets credentials are not valid: {exc}"
        return True, None

    async def iter_raw_rows(self) -> AsyncIterator[TabularRawRow]:
        rows = await self._fetch_rows()
        for idx, row in enumerate(rows, start=1):
            values = list(row) if isinstance(row, list) else [row]
            yield TabularRawRow(row_number=idx, values=values)

    def authorize(self, **kwargs) -> str:
        credentials: Credentials | None = None
        _allow_insecure_local_oauth_transport()

        if "authorization_response" in kwargs:
            self._ensure_client_secret_file()
            flow = Flow.from_client_secrets_file(
                self.client_secret_path, SCOPES, state=self._oauth_state
            )
            authorization_response = str(kwargs["authorization_response"])
            flow.redirect_uri = f"http://localhost:{PORT}/api/auth/{self.actor.id}"
            flow.fetch_token(authorization_response=authorization_response)
            credentials = flow.credentials
            self.token_path.write_text(credentials.to_json())
            self._credentials = credentials
            return "authorized"

        if self.token_path.exists():
            credentials = Credentials.from_authorized_user_file(self.token_path, SCOPES)
        if credentials is not None:
            self._credentials = credentials
            return "authorized"

        self._ensure_client_secret_file()
        flow = Flow.from_client_secrets_file(self.client_secret_path, SCOPES)
        flow.redirect_uri = f"http://localhost:{PORT}/api/auth/{self.actor.id}"
        authorization_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
            login_hint=self.account,
        )
        self._oauth_state = state
        return authorization_url

    def _ensure_client_secret_file(self) -> None:
        if self.client_secret_path.exists():
            return
        raise RuntimeError(
            "Missing OAuth client secret file. "
            f"Expected at '{self.client_secret_path}'. "
            "Download the OAuth client JSON from Google Cloud Console and place it there, "
            "or set config.client_secret_path to a valid file."
        )

    async def _fetch_rows(self) -> list[list[Any]]:
        encoded_range = quote(self._effective_range(), safe="!:$'(),")
        response = await self.http.get(
            f"/v4/spreadsheets/{self.spreadsheet_id}/values/{encoded_range}",
            params={
                "majorDimension": "ROWS",
                "valueRenderOption": "UNFORMATTED_VALUE",
                "dateTimeRenderOption": "FORMATTED_STRING",
            },
            headers=self._auth_headers(),
        )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(self._format_google_api_error(response)) from exc
        payload = response.json()
        rows = payload.get("values") or []
        if not isinstance(rows, list):
            raise ValueError("Google Sheets API response is missing a row list")
        return [row if isinstance(row, list) else [row] for row in rows]

    def _format_google_api_error(self, response: httpx.Response) -> str:
        """Return a compact but informative Google API error message."""
        status = response.status_code
        try:
            payload = response.json()
        except Exception:
            body = (response.text or "").strip()
            if body:
                return f"Google Sheets API request failed ({status}): {body[:1200]}"
            return f"Google Sheets API request failed ({status})"

        error_obj = payload.get("error") if isinstance(payload, dict) else None
        if not isinstance(error_obj, dict):
            return f"Google Sheets API request failed ({status})"

        message = str(error_obj.get("message") or "").strip()
        reason = None
        details = error_obj.get("details")
        if isinstance(details, list):
            for item in details:
                if not isinstance(item, dict):
                    continue
                maybe_reason = item.get("reason")
                if maybe_reason:
                    reason = str(maybe_reason).strip()
                    break

        parts: list[str] = [f"Google Sheets API request failed ({status})"]
        if reason:
            parts.append(f"reason={reason}")
        if message:
            parts.append(message)
        return parts[0] if len(parts) == 1 else f"{parts[0]} - {' | '.join(parts[1:])}"

    def _effective_range(self) -> str:
        if self.range_a1:
            return self.range_a1
        if self.worksheet:
            escaped = self.worksheet.replace("'", "''")
            return f"'{escaped}'!A:ZZ"
        return "A:ZZ"

    async def _load_credentials(self) -> Credentials:
        if self._credentials is not None:
            return self._credentials
        if not self.token_path.exists():
            raise RuntimeError("Google Sheets credentials are not valid")
        credentials = Credentials.from_authorized_user_file(self.token_path, SCOPES)
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(GoogleRequest())
                self.token_path.write_text(credentials.to_json())
            else:
                raise RuntimeError("Google Sheets credentials are not valid")
        self._credentials = credentials
        return credentials

    def _refresh_credentials_if_needed(self) -> None:
        if self._credentials is None:
            raise RuntimeError("Google Sheets credentials are not loaded")
        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(GoogleRequest())
            self.token_path.write_text(self._credentials.to_json())

    def _auth_headers(self) -> dict[str, str]:
        if self._credentials is None:
            raise RuntimeError(
                "Google Sheets credentials are not loaded. Run authorize() first."
            )
        self._refresh_credentials_if_needed()
        assert self._credentials is not None
        return {"Authorization": f"Bearer {self._credentials.token}"}

    async def scan(self) -> ScanResult:
        await self._load_credentials()
        logger.info(
            "Google Sheets scan connected spreadsheet_id={spreadsheet_id} worksheet={worksheet} range={range_a1}",
            spreadsheet_id=self.spreadsheet_id,
            worksheet=self.worksheet or "<default>",
            range_a1=self._effective_range(),
        )
        return await super().scan()
