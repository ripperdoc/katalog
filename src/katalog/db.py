from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from katalog.models import FileRecord, MetadataValue


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS assets (
        id TEXT PRIMARY KEY,
        title TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS asset_hashes (
        asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
        algorithm TEXT NOT NULL,
        hash_value TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (asset_id, algorithm)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY,
        title TEXT,
        source_type TEXT,
        plugin_id TEXT,
        config TEXT,
        last_scanned_at DATETIME,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS asset_versions (
        id TEXT PRIMARY KEY,
        asset_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
        label TEXT,
        kind TEXT NOT NULL CHECK (kind IN ('version','variant')),
        parent_version_id TEXT REFERENCES asset_versions(id),
        variant_of_version_id TEXT REFERENCES asset_versions(id),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (asset_id, label, kind)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS file_records (
        id TEXT PRIMARY KEY,
        asset_id TEXT REFERENCES assets(id) ON DELETE CASCADE,
        asset_version_id TEXT REFERENCES asset_versions(id) ON DELETE SET NULL,
        source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
        provider_file_id TEXT,
        canonical_uri TEXT NOT NULL,
        path TEXT,
        filename TEXT,
        size_bytes INTEGER,
        checksum_md5 TEXT,
        checksum_sha256 TEXT,
        mime_type TEXT,
        mtime DATETIME,
        ctime DATETIME,
        first_seen_at DATETIME NOT NULL,
        last_seen_at DATETIME NOT NULL,
        deleted_at DATETIME,
        UNIQUE (source_id, provider_file_id),
        UNIQUE (source_id, canonical_uri)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_file_records_asset ON file_records (asset_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_file_records_source ON file_records (source_id, last_seen_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS metadata_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id TEXT REFERENCES assets(id) ON DELETE CASCADE,
        file_record_id TEXT REFERENCES file_records(id) ON DELETE CASCADE,
        source_id TEXT REFERENCES sources(id),
        plugin_id TEXT NOT NULL,
        metadata_id TEXT NOT NULL,
        value_type TEXT NOT NULL CHECK (value_type IN ('string','int','float','datetime','json')),
        value_text TEXT,
        value_int INTEGER,
        value_real REAL,
        value_datetime DATETIME,
        value_json TEXT,
        confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence BETWEEN 0 AND 1),
        is_candidate INTEGER NOT NULL DEFAULT 1 CHECK (is_candidate IN (0,1)),
        version INTEGER,
        is_superseded INTEGER NOT NULL DEFAULT 0 CHECK (is_superseded IN (0,1)),
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (metadata_id, asset_id, file_record_id, plugin_id, value_type),
        CHECK (
            (value_text IS NOT NULL) +
            (value_int IS NOT NULL) +
            (value_real IS NOT NULL) +
            (value_datetime IS NOT NULL) +
            (value_json IS NOT NULL)
            = 1
        )
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON metadata_entries (metadata_id, value_type);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_metadata_asset ON metadata_entries (asset_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_metadata_plugin ON metadata_entries (plugin_id, updated_at DESC);
    """,
)


@dataclass(slots=True)
class ScanContext:
    source_id: str
    started_at: datetime


class Database:
    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    def initialize_schema(self) -> None:
        for statement in SCHEMA_STATEMENTS:
            self.conn.execute(statement)
        self.conn.commit()

    def ensure_source(
        self,
        source_id: str,
        *,
        title: str | None,
        source_type: str | None,
        plugin_id: str | None,
        config: dict | None,
    ) -> None:
        payload = json.dumps(config or {}, default=str)
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO sources (id, title, source_type, plugin_id, config, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                source_type=excluded.source_type,
                plugin_id=excluded.plugin_id,
                config=excluded.config,
                updated_at=excluded.updated_at
            """,
            (source_id, title, source_type, plugin_id, payload, now),
        )
        self.conn.commit()

    def begin_scan(self, source_id: str) -> ScanContext:
        started = datetime.now(timezone.utc)
        return ScanContext(source_id=source_id, started_at=started)

    def finalize_scan(self, ctx: ScanContext) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE file_records
            SET deleted_at = ?
            WHERE source_id = ? AND deleted_at IS NULL AND last_seen_at < ?
            """,
            (now_iso, ctx.source_id, ctx.started_at.isoformat()),
        )
        self.conn.execute(
            """
            UPDATE sources
            SET last_scanned_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now_iso, now_iso, ctx.source_id),
        )
        self.conn.commit()

    def upsert_file_record(self, record: FileRecord, ctx: ScanContext) -> str:
        record_id = record.id or record.provider_file_id or record.canonical_uri
        if not record_id:
            raise ValueError(
                "file record must have either provider_file_id or canonical_uri"
            )
        record.id = record_id
        last_seen = ctx.started_at.isoformat()
        first_seen = (
            record.first_seen_at.isoformat() if record.first_seen_at else last_seen
        )
        self.conn.execute(
            """
            INSERT INTO file_records (
                id, asset_id, asset_version_id, source_id, provider_file_id,
                canonical_uri, path, filename, size_bytes, checksum_md5, checksum_sha256,
                mime_type, mtime, ctime, first_seen_at, last_seen_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(id) DO UPDATE SET
                asset_id=excluded.asset_id,
                asset_version_id=excluded.asset_version_id,
                provider_file_id=excluded.provider_file_id,
                canonical_uri=excluded.canonical_uri,
                path=excluded.path,
                filename=excluded.filename,
                size_bytes=excluded.size_bytes,
                checksum_md5=excluded.checksum_md5,
                checksum_sha256=excluded.checksum_sha256,
                mime_type=excluded.mime_type,
                mtime=excluded.mtime,
                ctime=excluded.ctime,
                last_seen_at=excluded.last_seen_at,
                deleted_at=NULL
            """,
            (
                record_id,
                record.asset_id,
                record.asset_version_id,
                record.source_id,
                record.provider_file_id,
                record.canonical_uri,
                record.path,
                record.filename,
                record.size_bytes,
                record.checksum_md5,
                record.checksum_sha256,
                record.mime_type,
                self._maybe_iso(record.mtime),
                self._maybe_iso(record.ctime),
                first_seen,
                last_seen,
            ),
        )
        self.conn.commit()
        if record.metadata:
            self._insert_metadata(record_id, record, record.metadata)
        return record_id

    def _insert_metadata(
        self, file_record_id: str, record: FileRecord, metadata: Iterable[MetadataValue]
    ) -> None:
        for entry in metadata:
            columns = entry.as_sql_columns()
            value_json = columns["value_json"]
            if isinstance(value_json, (dict, list)):
                columns["value_json"] = json.dumps(value_json)
            self.conn.execute(
                """
                INSERT INTO metadata_entries (
                    asset_id,
                    file_record_id,
                    source_id,
                    plugin_id,
                    metadata_id,
                    value_type,
                    value_text,
                    value_int,
                    value_real,
                    value_datetime,
                    value_json,
                    confidence,
                    is_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metadata_id, asset_id, file_record_id, plugin_id, value_type)
                DO UPDATE SET
                    value_text=excluded.value_text,
                    value_int=excluded.value_int,
                    value_real=excluded.value_real,
                    value_datetime=excluded.value_datetime,
                    value_json=excluded.value_json,
                    confidence=excluded.confidence,
                    is_candidate=excluded.is_candidate,
                    updated_at=CURRENT_TIMESTAMP,
                    is_superseded=0
                """,
                (
                    record.asset_id,
                    file_record_id,
                    entry.source_id or record.source_id,
                    entry.plugin_id,
                    entry.metadata_id,
                    entry.value_type,
                    columns["value_text"],
                    columns["value_int"],
                    columns["value_real"],
                    columns["value_datetime"],
                    columns["value_json"],
                    entry.confidence,
                    1 if entry.is_candidate else 0,
                ),
            )
        self.conn.commit()

    @staticmethod
    def _maybe_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
