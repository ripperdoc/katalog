from __future__ import annotations

from collections import Counter
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Literal, TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover
    from katalog.analyzers.base import RelationshipRecord

from katalog.models import FileRecord, Metadata, MetadataKey


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
        metadata_key TEXT NOT NULL,
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
    CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON metadata_entries (metadata_key, value_type);
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

    def upsert_file_record(
        self, record: FileRecord, metadata: list[Metadata], snapshot: Snapshot
    ) -> set[str]:
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
        inserted = False
        with self._lock:
            try:
                self.conn.execute(
                    """-- sql
                INSERT INTO file_records (
                    id,
                    source_id,
                    canonical_uri,
                    created_snapshot_id,
                    last_snapshot_id,
                    deleted_snapshot_id
                ) VALUES (?, ?, ?, ?, ?, NULL)
                """,
                    (
                        record.id,
                        record.source_id,
                        record.canonical_uri,
                        created_snapshot_id,
                        last_snapshot_id,
                    ),
                )
                inserted = True
            except sqlite3.IntegrityError:
                self.conn.execute(
                    """-- sql
                UPDATE file_records
                SET canonical_uri = ?,
                    last_snapshot_id = ?,
                    deleted_snapshot_id = NULL
                WHERE id = ?
                """,
                    (
                        record.canonical_uri,
                        last_snapshot_id,
                        record.id,
                    ),
                )
            self.conn.commit()

        if metadata:
            changed_metadata = self._insert_metadata(
                snapshot.id, record.id, record, metadata
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
        metadata: Iterable[Metadata],
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
                    metadata_key,
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
                      AND existing.metadata_key = ?
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
                        entry.key,
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
                        entry.key,
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
                    changed_ids.add(entry.key)
            self.conn.commit()
        return changed_ids

    # Metadata views
    # Complete: metadata dict, keyed by each unique metadata key, has values that contains all metadata entries in a list
    # Latest: metadata dict, keyed by each unique metadata key, has values that contains the latest metadata per source ID
    # Canonical: TODO add a canonical view which only contains a selected canonical metadata entry per key?

    def list_records_with_metadata(
        self,
        *,
        source_id: Optional[str] = None,
        view: Literal["flat", "complete"] = "flat",
    ) -> dict:
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
                m.metadata_key AS metadata_metadata_key,
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
            ORDER BY f.id, m.id
        """
        with self._lock:
            rows = self.conn.execute(query).fetchall()
        if not rows:
            return {"records": []}
        result: list[dict[str, Any]] = []
        current_id: str | None = None
        current_record: dict[str, Any] | None = None
        current_metadata: list[Metadata] = []
        metadata_counter = Counter()
        stats: dict = {"metadata": metadata_counter}
        for row in rows:
            file_id = row["file_id"]
            if file_id != current_id:
                if current_record:
                    current_record["metadata"] = Metadata.list_to_dict_by_key(
                        current_metadata
                    )
                    current_metadata = []
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
            if metadata_entry:
                metadata_counter[metadata_entry.key] += 1
                current_metadata.append(metadata_entry)
        if current_record:
            result.append(current_record)
        stats["records"] = len(result)
        return {"stats": stats, "records": result}

    def get_metadata_for_file(
        self,
        file_record_id: str,
        *,
        source_id: str | None = None,
        snapshot_id: int | None = None,
        plugin_id: str | None = None,
        metadata_key: MetadataKey | None = None,
    ) -> list[Metadata]:
        """Fetch metadata rows for a single file record with optional filters."""

        query = """-- sql
            SELECT
                id,
                file_record_id,
                source_id,
                snapshot_id,
                plugin_id,
                metadata_key,
                value_type,
                value_text,
                value_int,
                value_real,
                value_datetime,
                value_json,
                confidence
            FROM metadata_entries
            WHERE file_record_id = ?
        """
        params: list[Any] = [file_record_id]
        if source_id is not None:
            query += " AND source_id = ?"
            params.append(source_id)
        if snapshot_id is not None:
            query += " AND snapshot_id = ?"
            params.append(snapshot_id)
        if plugin_id is not None:
            query += " AND plugin_id = ?"
            params.append(plugin_id)
        if metadata_key is not None:
            query += " AND metadata_key = ?"
            params.append(str(metadata_key))
        query += " ORDER BY snapshot_id DESC, id DESC"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        return [Metadata.from_sql_row(dict(row)) for row in rows]

    def insert_metadata_entries(
        self,
        entries: Iterable[Metadata],
        *,
        snapshot: Snapshot,
        default_source_id: str | None = None,
    ) -> int:
        """Insert metadata rows that were produced outside of a source scan."""

        source_fallback = default_source_id or snapshot.source_id
        if source_fallback is None:
            raise ValueError("A source id is required to insert metadata entries")
        inserted = 0
        with self._lock:
            for entry in entries:
                if not entry.file_record_id:
                    raise ValueError("Metadata entry missing file_record_id")
                entry_source_id = entry.source_id or source_fallback
                columns = entry.as_sql_columns()
                value_json = columns["value_json"]
                if value_json is not None and not isinstance(value_json, str):
                    columns["value_json"] = json.dumps(value_json, sort_keys=True)
                self.conn.execute(
                    """-- sql
                INSERT INTO metadata_entries (
                    file_record_id,
                    source_id,
                    snapshot_id,
                    plugin_id,
                    metadata_key,
                    value_type,
                    value_text,
                    value_int,
                    value_real,
                    value_datetime,
                    value_json,
                    confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        entry.file_record_id,
                        entry_source_id,
                        snapshot.id,
                        entry.plugin_id,
                        entry.key,
                        entry.value_type,
                        columns["value_text"],
                        columns["value_int"],
                        columns["value_real"],
                        columns["value_datetime"],
                        columns["value_json"],
                        entry.confidence,
                    ),
                )
                inserted += 1
            self.conn.commit()
        return inserted

    def replace_relationships(
        self,
        *,
        plugin_id: str,
        relationships: Iterable[RelationshipRecord],
    ) -> int:
        """Replace all relationships for a plugin with the provided records."""

        if not plugin_id:
            raise ValueError("plugin_id is required to store relationships")
        rows = list(relationships)
        with self._lock:
            self.conn.execute(
                """-- sql
            DELETE FROM file_relationships WHERE plugin_id = ?
            """,
                (plugin_id,),
            )
            for rel in rows:
                description = rel.description
                if rel.attributes:
                    payload = json.dumps(rel.attributes, default=str, sort_keys=True)
                    description = description or payload
                self.conn.execute(
                    """-- sql
            INSERT OR REPLACE INTO file_relationships (
                from_file_id,
                to_file_id,
                relationship_type,
                plugin_id,
                confidence,
                description
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                    (
                        rel.from_file_id,
                        rel.to_file_id,
                        rel.relationship_type,
                        rel.plugin_id or plugin_id,
                        rel.confidence,
                        description,
                    ),
                )
            self.conn.commit()
        return len(rows)

    def get_latest_metadata_by_key(
        self, metadata_key: MetadataKey
    ) -> list[tuple[str, Metadata]]:
        """Return the most recent metadata rows for active files matching the key."""

        query = """-- sql
            SELECT
                f.id AS file_id,
                f.source_id AS file_source_id,
                m.id AS metadata_entry_id,
                m.file_record_id AS metadata_file_record_id,
                m.source_id AS metadata_source_id,
                m.snapshot_id AS metadata_snapshot_id,
                m.plugin_id AS metadata_plugin_id,
                m.metadata_key AS metadata_metadata_key,
                m.value_type AS metadata_value_type,
                m.value_text AS metadata_value_text,
                m.value_int AS metadata_value_int,
                m.value_real AS metadata_value_real,
                m.value_datetime AS metadata_value_datetime,
                m.value_json AS metadata_value_json,
                m.confidence AS metadata_confidence
            FROM metadata_entries AS m
            JOIN file_records AS f
                ON m.file_record_id = f.id
            WHERE f.deleted_snapshot_id IS NULL
              AND m.metadata_key = ?
              AND m.snapshot_id = (
                    SELECT MAX(m2.snapshot_id)
                    FROM metadata_entries AS m2
                    WHERE m2.file_record_id = m.file_record_id
                      AND m2.source_id = m.source_id
                      AND m2.plugin_id = m.plugin_id
                      AND m2.metadata_key = m.metadata_key
                )
        """
        params = (str(metadata_key),)
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        results: list[tuple[str, Metadata]] = []
        for row in rows:
            metadata_entry = self._metadata_from_join_row(row)
            if metadata_entry is None:
                continue
            results.append((row["file_id"], metadata_entry))
        return results

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
    def _metadata_from_join_row(row: sqlite3.Row) -> Metadata | None:
        entry_id = row["metadata_entry_id"]
        if entry_id is None:
            return None
        payload = {
            "id": entry_id,
            "file_record_id": row["metadata_file_record_id"],
            "source_id": row["metadata_source_id"],
            "snapshot_id": row["metadata_snapshot_id"],
            "plugin_id": row["metadata_plugin_id"],
            "metadata_key": row["metadata_metadata_key"],
            "value_type": row["metadata_value_type"],
            "value_text": row["metadata_value_text"],
            "value_int": row["metadata_value_int"],
            "value_real": row["metadata_value_real"],
            "value_datetime": row["metadata_value_datetime"],
            "value_json": row["metadata_value_json"],
            "confidence": row["metadata_confidence"],
        }
        return Metadata.from_sql_row(payload)

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
