from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Literal, Mapping

from katalog.models import FileRecord, MetadataValue


SCHEMA_STATEMENTS = (
    """-- sql
    CREATE TABLE IF NOT EXISTS assets (
        id TEXT PRIMARY KEY,
        title TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY,
        title TEXT,
        plugin_id TEXT,
        config TEXT,
        last_scanned_at DATETIME,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS file_records (
        id TEXT PRIMARY KEY,
        asset_id TEXT REFERENCES assets(id) ON DELETE CASCADE,
        source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
        canonical_uri TEXT NOT NULL,
        first_seen_at DATETIME NOT NULL,
        last_seen_at DATETIME NOT NULL,
        deleted_at DATETIME,
        UNIQUE (source_id, canonical_uri)
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_file_records_asset ON file_records (asset_id);
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_file_records_source ON file_records (source_id, last_seen_at);
    """,
    """-- sql
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
        ),
        CHECK (asset_id IS NOT NULL OR file_record_id IS NOT NULL)
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON metadata_entries (metadata_id, value_type);
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_metadata_asset ON metadata_entries (asset_id);
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_metadata_plugin ON metadata_entries (plugin_id, updated_at DESC);
    """,
    """-- sql
    CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_dedupe
    ON metadata_entries (metadata_id, asset_id, file_record_id, plugin_id, value_type);
    """,
)


@dataclass(slots=True)
class ScanContext:
    source_id: str
    started_at: datetime


class Database:
    def __init__(self, db_path: str | Path):
        self.path = Path(db_path)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._lock = Lock()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def initialize_schema(self) -> None:
        with self._lock:
            for statement in SCHEMA_STATEMENTS:
                self.conn.execute(statement)
            self.conn.commit()

    def ensure_source(
        self,
        source_id: str,
        *,
        title: str | None,
        plugin_id: str | None,
        config: dict | None,
    ) -> None:
        payload = json.dumps(config or {}, default=str)
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute(
                """-- sql
            INSERT INTO sources (id, title, plugin_id, config, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title,
                plugin_id=excluded.plugin_id,
                config=excluded.config,
                updated_at=excluded.updated_at
            """,
                (source_id, title, plugin_id, payload, now),
            )
            self.conn.commit()

    def begin_scan(self, source_id: str) -> ScanContext:
        started = datetime.now(timezone.utc)
        return ScanContext(source_id=source_id, started_at=started)

    def finalize_scan(self, ctx: ScanContext) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute(
                """-- sql
            UPDATE file_records
            SET deleted_at = ?
            WHERE source_id = ? AND deleted_at IS NULL AND last_seen_at < ?
            """,
                (now_iso, ctx.source_id, ctx.started_at.isoformat()),
            )
            self.conn.execute(
                """-- sql
            UPDATE sources
            SET last_scanned_at = ?, updated_at = ?
            WHERE id = ?
            """,
                (now_iso, now_iso, ctx.source_id),
            )
            self.conn.commit()

    def upsert_file_record(self, record: FileRecord, ctx: ScanContext) -> str:
        if not record.id:
            raise ValueError("file record requires a stable id")
        if not record.canonical_uri:
            raise ValueError("file record requires a canonical_uri")
        last_seen = ctx.started_at.isoformat()
        first_seen = (
            record.first_seen_at.isoformat() if record.first_seen_at else last_seen
        )
        with self._lock:
            self.conn.execute(
                """-- sql
            INSERT INTO file_records (
                id, asset_id, source_id, canonical_uri, first_seen_at, last_seen_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            ON CONFLICT(id) DO UPDATE SET
                asset_id=excluded.asset_id,
                canonical_uri=excluded.canonical_uri,
                last_seen_at=excluded.last_seen_at,
                deleted_at=NULL
            """,
                (
                    record.id,
                    record.asset_id,
                    record.source_id,
                    record.canonical_uri,
                    first_seen,
                    last_seen,
                ),
            )
            self.conn.commit()
        if record.metadata:
            self._insert_metadata(record.id, record, record.metadata)
        return record.id

    def _insert_metadata(
        self, file_record_id: str, record: FileRecord, metadata: Iterable[MetadataValue]
    ) -> None:
        with self._lock:
            for entry in metadata:
                columns = entry.as_sql_columns()
                value_json = columns["value_json"]
                if isinstance(value_json, (dict, list)):
                    columns["value_json"] = json.dumps(value_json)
                self.conn.execute(
                    """-- sql
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
                    confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metadata_id, asset_id, file_record_id, plugin_id, value_type)
                DO UPDATE SET
                    value_text=excluded.value_text,
                    value_int=excluded.value_int,
                    value_real=excluded.value_real,
                    value_datetime=excluded.value_datetime,
                    value_json=excluded.value_json,
                    confidence=excluded.confidence,
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
                    ),
                )
            self.conn.commit()

    def list_files_with_metadata(
        self, source_id: str, *, view: Literal["flat", "complete"] = "flat"
    ) -> list[dict[str, Any]]:
        query = """-- sql
            SELECT
                f.id AS file_id,
                f.asset_id AS file_asset_id,
                f.source_id AS file_source_id,
                f.canonical_uri,
                f.first_seen_at,
                f.last_seen_at,
                f.deleted_at,
                m.id AS metadata_entry_id,
                m.asset_id AS metadata_asset_id,
                m.file_record_id AS metadata_file_record_id,
                m.source_id AS metadata_source_id,
                m.plugin_id AS metadata_plugin_id,
                m.metadata_id AS metadata_metadata_id,
                m.value_type AS metadata_value_type,
                m.value_text AS metadata_value_text,
                m.value_int AS metadata_value_int,
                m.value_real AS metadata_value_real,
                m.value_datetime AS metadata_value_datetime,
                m.value_json AS metadata_value_json,
                m.confidence AS metadata_confidence,
                m.version AS metadata_version,
                m.is_superseded AS metadata_is_superseded,
                m.updated_at AS metadata_updated_at
            FROM file_records AS f
            LEFT JOIN metadata_entries AS m
                ON m.file_record_id = f.id
            WHERE f.source_id = ?
            ORDER BY f.last_seen_at DESC, f.id, m.metadata_id, m.updated_at DESC, m.id
        """
        with self._lock:
            rows = self.conn.execute(query, (source_id,)).fetchall()
        if not rows:
            return []
        result: list[dict[str, Any]] = []
        current_id: str | None = None
        current_record: dict[str, Any] | None = None
        for row in rows:
            file_id = row["file_id"]
            if file_id != current_id:
                if current_record:
                    result.append(current_record)
                current_record = {
                    "id": file_id,
                    "asset_id": row["file_asset_id"],
                    "source_id": row["file_source_id"],
                    "canonical_uri": row["canonical_uri"],
                    "first_seen_at": row["first_seen_at"],
                    "last_seen_at": row["last_seen_at"],
                    "deleted_at": row["deleted_at"],
                    "metadata": {} if view == "flat" else [],
                }
                current_id = file_id
            metadata_entry = self._metadata_from_join_row(row)
            if metadata_entry and current_record is not None:
                if view == "complete":
                    current_record["metadata"].append(metadata_entry)
                else:
                    metadata_dict = current_record["metadata"]
                    metadata_id = metadata_entry["metadata_id"]
                    if metadata_id not in metadata_dict:
                        metadata_dict[metadata_id] = metadata_entry["value"]
        if current_record:
            result.append(current_record)
        return result

    @staticmethod
    def _metadata_from_join_row(row: sqlite3.Row) -> dict[str, Any] | None:
        entry_id = row["metadata_entry_id"]
        if entry_id is None:
            return None
        payload = {
            "id": entry_id,
            "asset_id": row["metadata_asset_id"],
            "file_record_id": row["metadata_file_record_id"],
            "source_id": row["metadata_source_id"],
            "plugin_id": row["metadata_plugin_id"],
            "metadata_id": row["metadata_metadata_id"],
            "value_type": row["metadata_value_type"],
            "value_text": row["metadata_value_text"],
            "value_int": row["metadata_value_int"],
            "value_real": row["metadata_value_real"],
            "value_datetime": row["metadata_value_datetime"],
            "value_json": row["metadata_value_json"],
            "confidence": row["metadata_confidence"],
            "version": row["metadata_version"],
            "is_superseded": row["metadata_is_superseded"],
            "updated_at": row["metadata_updated_at"],
        }
        return Database._coerce_metadata_row(payload)

    @staticmethod
    def _coerce_metadata_row(row: Mapping[str, Any]) -> dict[str, Any]:
        value: Any
        if row["value_text"] is not None:
            value = row["value_text"]
        elif row["value_int"] is not None:
            value = row["value_int"]
        elif row["value_real"] is not None:
            value = row["value_real"]
        elif row["value_datetime"] is not None:
            value = row["value_datetime"]
        elif row["value_json"] is not None:
            try:
                value = json.loads(row["value_json"])
            except json.JSONDecodeError:
                value = row["value_json"]
        else:
            value = None
        return {
            "id": row["id"],
            "asset_id": row["asset_id"],
            "file_record_id": row["file_record_id"],
            "source_id": row["source_id"],
            "plugin_id": row["plugin_id"],
            "metadata_id": row["metadata_id"],
            "value_type": row["value_type"],
            "value": value,
            "confidence": row["confidence"],
            "version": row["version"],
            "is_superseded": bool(row["is_superseded"]),
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _maybe_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
