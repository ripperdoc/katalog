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
    CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY,
        title TEXT,
        plugin_id TEXT,
        config TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY,
        source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
        started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at DATETIME,
        is_partial INTEGER NOT NULL DEFAULT 0 CHECK (is_partial IN (0,1)),
        metadata TEXT
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots (source_id, id);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS file_records (
        id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
        canonical_uri TEXT NOT NULL,
        created_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE RESTRICT,
        last_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE RESTRICT,
        deleted_snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE SET NULL,
        UNIQUE (source_id, canonical_uri)
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_file_records_source ON file_records (source_id, last_snapshot_id);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS metadata_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_record_id TEXT REFERENCES file_records(id) ON DELETE CASCADE,
        source_id TEXT REFERENCES sources(id),
        snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
        plugin_id TEXT NOT NULL,
        metadata_id TEXT NOT NULL,
        value_type TEXT NOT NULL CHECK (value_type IN ('string','int','float','datetime','json')),
        value_text TEXT,
        value_int INTEGER,
        value_real REAL,
        value_datetime DATETIME,
        value_json TEXT,
        confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence BETWEEN 0 AND 1),
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
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON metadata_entries (metadata_id, value_type);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS file_relationships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_file_id TEXT NOT NULL REFERENCES file_records(id) ON DELETE CASCADE,
        to_file_id TEXT NOT NULL REFERENCES file_records(id) ON DELETE CASCADE,
        relationship_type TEXT NOT NULL,
        plugin_id TEXT,
        confidence REAL,
        description TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (from_file_id, to_file_id, relationship_type)
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_relationships_type ON file_relationships (relationship_type);
    """,
)


@dataclass(slots=True)
class Snapshot:
    id: int
    source_id: str
    started_at: datetime
    is_partial: bool = False


@dataclass(slots=True)
class FileRelationship:
    id: int
    from_file_id: str
    to_file_id: str
    relationship_type: str
    plugin_id: str | None
    confidence: float | None
    description: str | None
    created_at: datetime


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

    def begin_snapshot(
        self,
        source_id: str,
        *,
        partial: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Snapshot:
        started = datetime.now(timezone.utc)
        snapshot_id = self._generate_snapshot_id(started)
        payload = json.dumps(metadata or {}, default=str) if metadata else None
        with self._lock:
            while True:
                try:
                    self.conn.execute(
                        """-- sql
                    INSERT INTO snapshots (id, source_id, started_at, is_partial, metadata)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            snapshot_id,
                            source_id,
                            started.isoformat(),
                            int(partial),
                            payload,
                        ),
                    )
                    self.conn.commit()
                    break
                except sqlite3.IntegrityError:
                    snapshot_id += 1
        return Snapshot(
            id=snapshot_id, source_id=source_id, started_at=started, is_partial=partial
        )

    def finalize_snapshot(
        self, snapshot: Snapshot, *, partial: bool | None = None
    ) -> None:
        completed_iso = datetime.now(timezone.utc).isoformat()
        partial_flag = int(partial if partial is not None else snapshot.is_partial)
        with self._lock:
            self.conn.execute(
                """-- sql
            UPDATE snapshots
            SET completed_at = ?, is_partial = ?
            WHERE id = ?
            """,
                (completed_iso, partial_flag, snapshot.id),
            )
            self.conn.execute(
                """-- sql
            UPDATE file_records
            SET deleted_snapshot_id = ?
            WHERE source_id = ?
              AND deleted_snapshot_id IS NULL
              AND last_snapshot_id < ?
            """,
                (snapshot.id, snapshot.source_id, snapshot.id),
            )
            self.conn.execute(
                """-- sql
            UPDATE sources
            SET updated_at = ?
            WHERE id = ?
            """,
                (completed_iso, snapshot.source_id),
            )
            self.conn.commit()
        snapshot.is_partial = bool(partial_flag)

    def upsert_file_record(self, record: FileRecord, snapshot: Snapshot) -> set[str]:
        if not record.id:
            raise ValueError("file record requires a stable id")
        if not record.canonical_uri:
            raise ValueError("file record requires a canonical_uri")
        if record.source_id != snapshot.source_id:
            raise ValueError(
                "file record source mismatch: %s vs %s"
                % (record.source_id, snapshot.source_id)
            )
        created_snapshot_id = record.created_snapshot_id or snapshot.id
        last_snapshot_id = snapshot.id
        with self._lock:
            cursor = self.conn.execute(
                """-- sql
            WITH upsert AS (
                INSERT INTO file_records (
                    id,
                    source_id,
                    canonical_uri,
                    created_snapshot_id,
                    last_snapshot_id,
                    deleted_snapshot_id
                ) VALUES (?, ?, ?, ?, ?, NULL)
                ON CONFLICT(id) DO UPDATE SET
                    canonical_uri=excluded.canonical_uri,
                    last_snapshot_id=excluded.last_snapshot_id,
                    deleted_snapshot_id=NULL
                RETURNING
                    id,
                    created_snapshot_id
            )
            SELECT
                upsert.id AS file_id,
                CASE
                    WHEN upsert.created_snapshot_id = ? THEN 1
                    ELSE 0
                END AS inserted
            FROM upsert
            """,
                (
                    record.id,
                    record.source_id,
                    record.canonical_uri,
                    created_snapshot_id,
                    last_snapshot_id,
                    snapshot.id,
                ),
            )
            row = cursor.fetchone()
            inserted = bool(row[1]) if row else False
            self.conn.commit()

        if record.metadata:
            changed_metadata = self._insert_metadata(
                snapshot.id, record.id, record, record.metadata
            )
        else:
            changed_metadata: set[str] = set()
        if inserted:
            # Signals that the file record itself was created
            changed_metadata.add("file_record")
        return changed_metadata

    def _insert_metadata(
        self,
        snapshot_id: int,
        file_record_id: str,
        record: FileRecord,
        metadata: Iterable[MetadataValue],
    ) -> set[str]:
        changed_ids: set[str] = set()
        with self._lock:
            for entry in metadata:
                columns = entry.as_sql_columns()
                value_json = columns["value_json"]
                if value_json is not None and not isinstance(value_json, str):
                    columns["value_json"] = json.dumps(value_json, sort_keys=True)
                entry_source_id = entry.source_id or record.source_id
                cursor = self.conn.execute(
                    """-- sql
                INSERT INTO metadata_entries (
                    file_record_id,
                    source_id,
                    snapshot_id,
                    plugin_id,
                    metadata_id,
                    value_type,
                    value_text,
                    value_int,
                    value_real,
                    value_datetime,
                    value_json,
                    confidence
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM metadata_entries AS existing
                    WHERE existing.file_record_id = ?
                      AND existing.source_id = ?
                      AND existing.plugin_id = ?
                      AND existing.metadata_id = ?
                      AND existing.value_type = ?
                      AND existing.value_text IS ?
                      AND existing.value_int IS ?
                      AND existing.value_real IS ?
                      AND existing.value_datetime IS ?
                      AND existing.value_json IS ?
                      AND existing.confidence = ?
                )
                """,
                    (
                        file_record_id,
                        entry_source_id,
                        snapshot_id,
                        entry.plugin_id,
                        entry.metadata_id,
                        entry.value_type,
                        columns["value_text"],
                        columns["value_int"],
                        columns["value_real"],
                        columns["value_datetime"],
                        columns["value_json"],
                        entry.confidence,
                        file_record_id,
                        entry_source_id,
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
                if cursor.rowcount == 1:
                    changed_ids.add(entry.metadata_id)
            self.conn.commit()
        return changed_ids

    def list_files_with_metadata(
        self, source_id: str, *, view: Literal["flat", "complete"] = "flat"
    ) -> list[dict[str, Any]]:
        query = """-- sql
            SELECT
                f.id AS file_id,
                f.source_id AS file_source_id,
                f.canonical_uri,
                f.created_snapshot_id,
                f.last_snapshot_id,
                f.deleted_snapshot_id,
                m.id AS metadata_entry_id,
                m.file_record_id AS metadata_file_record_id,
                m.source_id AS metadata_source_id,
                m.snapshot_id AS metadata_snapshot_id,
                m.plugin_id AS metadata_plugin_id,
                m.metadata_id AS metadata_metadata_id,
                m.value_type AS metadata_value_type,
                m.value_text AS metadata_value_text,
                m.value_int AS metadata_value_int,
                m.value_real AS metadata_value_real,
                m.value_datetime AS metadata_value_datetime,
                m.value_json AS metadata_value_json,
                m.confidence AS metadata_confidence
            FROM file_records AS f
            LEFT JOIN metadata_entries AS m
                ON m.file_record_id = f.id
            WHERE f.source_id = ?
            ORDER BY f.last_snapshot_id DESC, f.id, m.metadata_id, m.id
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
                    "source_id": row["file_source_id"],
                    "canonical_uri": row["canonical_uri"],
                    "created_snapshot_id": row["created_snapshot_id"],
                    "last_snapshot_id": row["last_snapshot_id"],
                    "deleted_snapshot_id": row["deleted_snapshot_id"],
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

    def list_relationships(
        self,
        *,
        source_id: str | None = None,
        file_id: str | None = None,
    ) -> list[FileRelationship]:
        query = """-- sql
            SELECT
                r.id,
                r.from_file_id,
                r.to_file_id,
                r.relationship_type,
                r.plugin_id,
                r.confidence,
                r.description,
                r.created_at
            FROM file_relationships AS r
            JOIN file_records AS f_from ON f_from.id = r.from_file_id
            JOIN file_records AS f_to ON f_to.id = r.to_file_id
        """
        params: list[Any] = []
        clauses: list[str] = []
        if source_id:
            clauses.append("(f_from.source_id = ? OR f_to.source_id = ?)")
            params.extend([source_id, source_id])
        if file_id:
            clauses.append("(r.from_file_id = ? OR r.to_file_id = ?)")
            params.extend([file_id, file_id])
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY r.created_at DESC, r.id DESC"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        return [
            FileRelationship(
                id=row["id"],
                from_file_id=row["from_file_id"],
                to_file_id=row["to_file_id"],
                relationship_type=row["relationship_type"],
                plugin_id=row["plugin_id"],
                confidence=row["confidence"],
                description=row["description"],
                created_at=datetime.fromisoformat(row["created_at"])
                if isinstance(row["created_at"], str)
                else row["created_at"],
            )
            for row in rows
        ]

    @staticmethod
    def _metadata_from_join_row(row: sqlite3.Row) -> dict[str, Any] | None:
        entry_id = row["metadata_entry_id"]
        if entry_id is None:
            return None
        payload = {
            "id": entry_id,
            "file_record_id": row["metadata_file_record_id"],
            "source_id": row["metadata_source_id"],
            "snapshot_id": row["metadata_snapshot_id"],
            "plugin_id": row["metadata_plugin_id"],
            "metadata_id": row["metadata_metadata_id"],
            "value_type": row["metadata_value_type"],
            "value_text": row["metadata_value_text"],
            "value_int": row["metadata_value_int"],
            "value_real": row["metadata_value_real"],
            "value_datetime": row["metadata_value_datetime"],
            "value_json": row["metadata_value_json"],
            "confidence": row["metadata_confidence"],
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
            "file_record_id": row["file_record_id"],
            "source_id": row["source_id"],
            "snapshot_id": row["snapshot_id"],
            "plugin_id": row["plugin_id"],
            "metadata_id": row["metadata_id"],
            "value_type": row["value_type"],
            "value": value,
            "confidence": row["confidence"],
        }

    @staticmethod
    def _maybe_iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _generate_snapshot_id(moment: datetime) -> int:
        return int(moment.timestamp() * 1000)
