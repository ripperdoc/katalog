from __future__ import annotations

from collections import Counter
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Literal, TYPE_CHECKING, Optional
from time import perf_counter
import line_profiler

from loguru import logger

if TYPE_CHECKING:
    from katalog.analyzers.base import RelationshipRecord

from katalog.models import (
    Metadata,
    MetadataKey,
    get_metadata_schema,
    AssetRelationship,
    Snapshot,
    SnapshotStats,
)


SCHEMA_STATEMENTS = (
    """-- sql
    CREATE TABLE IF NOT EXISTS providers (
        id INTEGER PRIMARY KEY,
        title TEXT UNIQUE NOT NULL,
        plugin_id TEXT,
        config TEXT,
        type TEXT NOT NULL CHECK (type IN ('source','processor','analyzer','editor', 'exporter')),
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY,
        provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
        started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at DATETIME,
        status TEXT NOT NULL CHECK (status IN ('in_progress','partial','full', 'failed', 'canceled')),
        metadata TEXT
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_snapshots_source ON snapshots (provider_id, id);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS assets (
        id INTEGER PRIMARY KEY,
        provider_id INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
        canonical_id TEXT UNIQUE NOT NULL,
        canonical_uri TEXT UNIQUE NOT NULL,
        created_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE RESTRICT,
        last_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE RESTRICT,
        deleted_snapshot_id INTEGER REFERENCES snapshots(id) ON DELETE SET NULL
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_assets_source ON assets (provider_id, last_snapshot_id);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asset_id INTEGER REFERENCES assets(id) ON DELETE CASCADE,
        provider_id INTEGER NOT NULL REFERENCES providers(id),
        snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
        metadata_key TEXT NOT NULL,
        value_type TEXT NOT NULL CHECK (value_type IN ('string','int','float','datetime','json')),
        value_text TEXT,
        value_int INTEGER,
        value_real REAL,
        value_datetime DATETIME,
        value_json TEXT,
        removed INTEGER NOT NULL DEFAULT 0 CHECK (removed IN (0,1)),
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
    CREATE INDEX IF NOT EXISTS idx_metadata_lookup ON metadata (metadata_key, value_type);
    """,
    # Supports _load_current_metadata_state by letting SQLite seek on asset/provider and reuse
    # the ordering needed for metadata comparisons.
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_metadata_asset_provider
        ON metadata (asset_id, provider_id, metadata_key, snapshot_id DESC, id DESC);
    """,
    """-- sql
    CREATE TABLE IF NOT EXISTS asset_relationships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
        from_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
        to_id TEXT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
        relationship_type TEXT NOT NULL,
        snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
        removed INTEGER NOT NULL DEFAULT 0 CHECK (removed IN (0,1)),
        confidence REAL,
        description TEXT
    );
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_relationships_type ON asset_relationships (relationship_type);
    """,
    """-- sql
    CREATE INDEX IF NOT EXISTS idx_relationships_identity
        ON asset_relationships (provider_id, from_id, to_id, relationship_type, removed, snapshot_id);
    """,
)


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
        provider_id: str,
        *,
        title: str | None,
        plugin_id: str | None,
        config: dict | None,
        provider_type: str = "source",
    ) -> None:
        payload = json.dumps(config or {}, default=str)
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.execute(
                """-- sql
            INSERT INTO providers (id, type, title, plugin_id, config, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                type=excluded.type,
                title=excluded.title,
                plugin_id=excluded.plugin_id,
                config=excluded.config,
                updated_at=excluded.updated_at
            """,
                (provider_id, provider_type, title, plugin_id, payload, now),
            )
            self.conn.commit()

    def list_providers(self, provider_type: str | None = None) -> list[dict[str, Any]]:
        """Return provider rows with parsed config payloads."""

        query = """-- sql
            SELECT id, type, title, plugin_id, config
            FROM providers
        """
        params: list[Any] = []
        if provider_type is not None:
            query += " WHERE type = ?"
            params.append(provider_type)
        query += " ORDER BY updated_at DESC, id"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        providers: list[dict[str, Any]] = []
        for row in rows:
            config_payload = row["config"] or "{}"
            try:
                parsed_config = json.loads(config_payload)
            except json.JSONDecodeError:
                parsed_config = {}
            providers.append(
                {
                    "id": row["id"],
                    "type": row["type"],
                    "title": row["title"],
                    "plugin_id": row["plugin_id"],
                    "config": parsed_config,
                }
            )
        return providers

    def begin_snapshot(
        self,
        provider: Provider,
        *,
        status: str = "in_progress",
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
                    INSERT INTO snapshots (id, provider_id, started_at, status, metadata)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            snapshot_id,
                            provider_id,
                            started.isoformat(),
                            status,
                            payload,
                        ),
                    )
                    self.conn.commit()
                    break
                except sqlite3.IntegrityError:
                    snapshot_id += 1
        return Snapshot(
            id=snapshot_id,
            provider_id=provider_id,
            started_at=started,
            status=status,
            completed_at=None,
            metadata=metadata,
        )

    def get_latest_snapshot(
        self,
        provider_id: str,
        *,
        statuses: tuple[str, ...] = ("full", "partial"),
    ) -> Snapshot | None:
        """Return the most recent completed snapshot for the provider."""

        query = """-- sql
            SELECT id, provider_id, started_at, completed_at, status, metadata
            FROM snapshots
            WHERE provider_id = ?
              AND completed_at IS NOT NULL
        """
        params: list[Any] = [provider_id]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            query += f" AND status IN ({placeholders})"
            params.extend(statuses)
        query += " ORDER BY completed_at DESC, id DESC LIMIT 1"
        with self._lock:
            row = self.conn.execute(query, params).fetchone()
        if not row:
            return None
        started = datetime.fromisoformat(row["started_at"])
        completed_raw = row["completed_at"]
        completed = (
            datetime.fromisoformat(completed_raw) if completed_raw is not None else None
        )
        metadata_raw = row["metadata"]
        metadata = None
        if metadata_raw:
            try:
                metadata = json.loads(metadata_raw)
            except json.JSONDecodeError:
                metadata = None
        return Snapshot(
            id=row["id"],
            provider_id=row["provider_id"],
            started_at=started,
            status=row["status"],
            completed_at=completed,
            metadata=metadata,
        )

    def list_snapshots(
        self,
        provider_id: str,
        *,
        statuses: tuple[str, ...] | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> list[Snapshot]:
        """Return all snapshots for a provider ordered by snapshot id."""

        query = """-- sql
            SELECT id, provider_id, started_at, completed_at, status, metadata
            FROM snapshots
            WHERE provider_id = ?
        """
        params: list[Any] = [provider_id]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            query += f" AND status IN ({placeholders})"
            params.extend(statuses)
        direction = "ASC" if order == "asc" else "DESC"
        query += f" ORDER BY id {direction}"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        snapshots: list[Snapshot] = []
        for row in rows:
            started = datetime.fromisoformat(row["started_at"])
            completed_raw = row["completed_at"]
            completed = (
                datetime.fromisoformat(completed_raw)
                if completed_raw is not None
                else None
            )
            metadata_raw = row["metadata"]
            metadata = None
            if metadata_raw:
                try:
                    metadata = json.loads(metadata_raw)
                except json.JSONDecodeError:
                    metadata = None
            snapshots.append(
                Snapshot(
                    id=row["id"],
                    provider_id=row["provider_id"],
                    started_at=started,
                    status=row["status"],
                    completed_at=completed,
                    metadata=metadata,
                )
            )
        return snapshots

    def get_cutoff_snapshot(self, provider_id: str) -> Snapshot | None:
        """Return the latest snapshot usable as an incremental cutoff.

        Requires at least one full snapshot; once a full has been seen, partial
        snapshots that follow it are also eligible. Canceled/failed snapshots are
        ignored.
        """

        snapshots = self.list_snapshots(
            provider_id, statuses=("full", "partial"), order="asc"
        )
        full_seen = False
        candidate: Snapshot | None = None
        for snapshot in snapshots:
            if snapshot.status == "full":
                full_seen = True
                candidate = snapshot
            elif snapshot.status == "partial" and full_seen:
                candidate = snapshot
        return candidate

    def finalize_snapshot(
        self, snapshot: Snapshot, *, status: str, stats: SnapshotStats | None = None
    ) -> None:
        completed_at = datetime.now(timezone.utc)
        completed_iso = completed_at.isoformat()
        metadata_payload: str | None = None
        if stats is not None or snapshot.metadata is not None:
            merged_metadata = dict(snapshot.metadata or {})
            if stats is not None:
                merged_metadata["stats"] = stats.to_dict()
            metadata_payload = json.dumps(merged_metadata, default=str)
            snapshot.metadata = merged_metadata
        with self._lock:
            params: list[Any] = [completed_iso, status]
            set_clause = "completed_at = ?, status = ?"
            if metadata_payload is not None:
                set_clause += ", metadata = ?"
                params.append(metadata_payload)
            params.append(snapshot.id)
            self.conn.execute(
                f"""-- sql
            UPDATE snapshots
            SET {set_clause}
            WHERE id = ?
            """,
                params,
            )
            self.conn.execute(
                """-- sql
            UPDATE assets
            SET deleted_snapshot_id = ?
            WHERE provider_id = ?
              AND deleted_snapshot_id IS NULL
              AND last_snapshot_id < ?
            """,
                (snapshot.id, snapshot.provider_id, snapshot.id),
            )
            self.conn.execute(
                """-- sql
            UPDATE providers
            SET updated_at = ?
            WHERE id = ?
            """,
                (completed_iso, snapshot.provider_id),
            )
            self.conn.commit()
        snapshot.status = status
        snapshot.completed_at = completed_at

    def upsert_asset(
        self,
        record: AssetRecord,
        metadata: list[Metadata],
        snapshot: Snapshot,
        stats: SnapshotStats | None = None,
    ) -> set[str]:
        start_time = perf_counter()
        if not record.id:
            raise ValueError("file record requires a stable id")
        if not record.canonical_uri:
            raise ValueError("file record requires a canonical_uri")
        if record.provider_id != snapshot.provider_id:
            raise ValueError(
                "file record source mismatch: %s vs %s"
                % (record.provider_id, snapshot.provider_id)
            )
        if not metadata:
            raise ValueError("upsert_asset requires at least one metadata entry")
        created_snapshot_id = record.created_snapshot_id or snapshot.id
        last_snapshot_id = snapshot.id
        inserted = False
        with self._lock:
            try:
                self.conn.execute(
                    """-- sql
                INSERT INTO assets (
                    id,
                    provider_id,
                    canonical_uri,
                    created_snapshot_id,
                    last_snapshot_id,
                    deleted_snapshot_id
                ) VALUES (?, ?, ?, ?, ?, NULL)
                """,
                    (
                        record.id,
                        record.provider_id,
                        record.canonical_uri,
                        created_snapshot_id,
                        last_snapshot_id,
                    ),
                )
                inserted = True
            except sqlite3.IntegrityError:
                self.conn.execute(
                    """-- sql
                UPDATE assets
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
                snapshot.id, record.id, record, metadata, stats=stats
            )
        else:
            changed_metadata: set[str] = set()
        if inserted:
            # Signals that the file record itself was created
            changed_metadata.add("asset")
        if stats:
            if inserted:
                stats.record_asset_change(record.id, added=True)
            elif changed_metadata:
                stats.record_asset_change(record.id, added=False)
        duration_ms = (perf_counter() - start_time) * 1000
        logger.debug(
            "Upserted asset {asset} with {metadata_count} metadata entries; {changed_count} keys changed in {duration:.2f} ms",
            asset=record.id,
            metadata_count=len(metadata),
            changed_count=len(changed_metadata),
            duration=duration_ms,
        )
        return changed_metadata

    def _insert_metadata(
        self,
        snapshot_id: int,
        asset_id: str,
        record: AssetRecord,
        metadata: Iterable[Metadata],
        *,
        stats: SnapshotStats | None = None,
    ) -> set[str]:
        grouped_values: dict[
            tuple[int, MetadataKey], dict[tuple[int, Any], dict[int, Any]]
        ] = {}
        provider_scope: set[int] = set()
        cleared_combos: set[tuple[int, MetadataKey]] = set()
        total_additions = 0
        total_removals = 0
        for entry in metadata:
            entry_provider_id = entry.provider_id or record.provider_id
            provider_scope.add(entry_provider_id)
            combo = (entry_provider_id, entry.key)

            if entry.value is None:
                grouped_values.setdefault(combo, {})
                cleared_combos.add(combo)
                continue

            if combo in cleared_combos:
                raise ValueError(
                    "Cannot set metadata values for %s after requesting a clear"
                    % str(entry.key)
                )

            columns = entry.as_sql_columns()
            value_json = columns["value_json"]
            if value_json is not None and not isinstance(value_json, str):
                columns["value_json"] = json.dumps(value_json, sort_keys=True)
            normalized_value = self._normalize_value(entry.value_type, columns)
            bucket = grouped_values.setdefault(combo, {})
            if normalized_value in bucket:
                continue
            bucket[normalized_value] = {
                "value_type": entry.value_type,
                "columns": columns,
                "confidence": entry.confidence,
            }

        existing_state = self._load_current_metadata_state(asset_id, provider_scope)
        combos = set(existing_state.keys()) | set(grouped_values.keys())
        if not combos:
            return set()

        changed_ids: set[str] = set()
        rows_to_insert: list[tuple[Any, ...]] = []
        for combo in combos:
            provider_id, metadata_key = combo
            incoming_values = grouped_values.get(combo, {})
            existing_values = existing_state.get(combo, {})
            active_values = {
                norm: payload
                for norm, payload in existing_values.items()
                if not payload["removed"]
            }

            incoming_keys = set(incoming_values.keys())
            active_keys = set(active_values.keys())
            additions = incoming_keys - active_keys
            removals = active_keys - incoming_keys

            if additions or removals:
                changed_ids.add(str(metadata_key))
                total_additions += len(additions)
                total_removals += len(removals)

            for normalized_value in additions:
                payload = incoming_values[normalized_value]
                rows_to_insert.append(
                    (
                        asset_id,
                        provider_id,
                        snapshot_id,
                        metadata_key,
                        payload["value_type"],
                        payload["columns"]["value_text"],
                        payload["columns"]["value_int"],
                        payload["columns"]["value_real"],
                        payload["columns"]["value_datetime"],
                        payload["columns"]["value_json"],
                        payload["confidence"],
                        0,
                    )
                )

            for normalized_value in removals:
                payload = active_values[normalized_value]
                rows_to_insert.append(
                    (
                        asset_id,
                        provider_id,
                        snapshot_id,
                        metadata_key,
                        payload["value_type"],
                        payload["columns"]["value_text"],
                        payload["columns"]["value_int"],
                        payload["columns"]["value_real"],
                        payload["columns"]["value_datetime"],
                        payload["columns"]["value_json"],
                        payload["confidence"],
                        1,
                    )
                )

        if not rows_to_insert:
            if stats and (total_additions or total_removals):
                stats.record_metadata_diff(total_additions, total_removals)
            return changed_ids

        with self._lock:
            self.conn.executemany(
                """-- sql
            INSERT INTO metadata (
                asset_id,
                provider_id,
                snapshot_id,
                metadata_key,
                value_type,
                value_text,
                value_int,
                value_real,
                value_datetime,
                value_json,
                confidence,
                removed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                rows_to_insert,
            )
            self.conn.commit()
        if stats and (total_additions or total_removals):
            stats.record_metadata_diff(total_additions, total_removals)
        return changed_ids

    @line_profiler.profile
    def _load_current_metadata_state(
        self, asset_id: str, provider_ids: set[str]
    ) -> dict[tuple[str, MetadataKey], dict[tuple[str, Any], dict[str, Any]]]:
        if not provider_ids:
            return {}
        ordered_providers = sorted(provider_ids)
        placeholders = ",".join("?" for _ in ordered_providers)
        query = f"""-- sql
            SELECT
                provider_id,
                metadata_key,
                value_type,
                value_text,
                value_int,
                value_real,
                value_datetime,
                value_json,
                confidence,
                removed,
                snapshot_id,
                id
            FROM metadata
            WHERE asset_id = ?
              AND provider_id IN ({placeholders})
            ORDER BY metadata_key, snapshot_id DESC, id DESC
        """
        params: list[Any] = [asset_id]
        params.extend(ordered_providers)

        with self._lock:
            # plan = self.conn.execute("EXPLAIN QUERY PLAN " + query, params).fetchall()
            # for row in plan:
            #     logger.debug(f"plan row: {dict(row)}")
            rows = self.conn.execute(query, params).fetchall()
        current: dict[
            tuple[str, MetadataKey], dict[tuple[str, Any], dict[str, Any]]
        ] = {}
        seen: set[tuple[tuple[str, MetadataKey], tuple[str, Any]]] = set()
        for row in rows:
            provider_id = row["provider_id"]
            metadata_key = MetadataKey(row["metadata_key"])
            columns = {
                "value_text": row["value_text"],
                "value_int": row["value_int"],
                "value_real": row["value_real"],
                "value_datetime": row["value_datetime"],
                "value_json": row["value_json"],
            }
            normalized_value = self._normalize_value(row["value_type"], columns)
            combo = (provider_id, metadata_key)
            combo_key = (combo, normalized_value)
            if combo_key in seen:
                continue
            seen.add(combo_key)
            confidence = row["confidence"]
            payload = {
                "value_type": row["value_type"],
                "columns": columns,
                "confidence": float(confidence if confidence is not None else 1.0),
                "removed": bool(row["removed"]),
                "snapshot_id": row["snapshot_id"],
            }
            current.setdefault(combo, {})[normalized_value] = payload
        return current

    @staticmethod
    def _normalize_value(value_type: str, columns: dict[str, Any]) -> tuple[str, Any]:
        if value_type == "string":
            return (value_type, columns.get("value_text"))
        if value_type == "int":
            raw = columns.get("value_int")
            if raw is None:
                raise ValueError("Integer metadata is missing value_int")
            return (value_type, int(raw))
        if value_type == "float":
            raw = columns.get("value_real")
            return (value_type, float(raw) if raw is not None else None)
        if value_type == "datetime":
            return (value_type, columns.get("value_datetime"))
        if value_type == "json":
            return (value_type, columns.get("value_json"))
        raise ValueError(f"Unsupported metadata value_type: {value_type}")

    def _load_relationship_state(
        self, provider_ids: set[str]
    ) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        if not provider_ids:
            return {}
        ordered_providers = sorted(provider_ids)
        placeholders = ",".join("?" for _ in ordered_providers)
        query = f"""-- sql
            SELECT
                provider_id,
                from_id,
                to_id,
                relationship_type,
                removed,
                snapshot_id
            FROM asset_relationships
            WHERE provider_id IN ({placeholders})
            ORDER BY snapshot_id DESC, id DESC
        """
        params: list[Any] = ordered_providers
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        state: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (
                row["provider_id"],
                row["from_id"],
                row["to_id"],
                row["relationship_type"],
            )
            if key in state:
                continue
            state[key] = {
                "removed": bool(row["removed"]),
                "snapshot_id": row["snapshot_id"],
            }
        return state

    # Metadata views
    # Complete: metadata dict, keyed by each unique metadata key, has values that contains all metadata entries in a list
    # Latest: metadata dict, keyed by each unique metadata key, has values that contains the latest metadata per source ID
    # Canonical: TODO add a canonical view which only contains a selected canonical metadata entry per key?

    def list_records_with_metadata(
        self,
        *,
        provider_id: Optional[str] = None,
        view: Literal["flat", "complete"] = "flat",
    ) -> dict:
        query = """-- sql
            SELECT
                f.id AS file_id,
                f.provider_id AS file_provider_id,
                f.canonical_uri,
                f.created_snapshot_id,
                f.last_snapshot_id,
                f.deleted_snapshot_id,
                m.id AS metadata_entry_id,
                m.asset_id AS metadata_asset_id,
                m.provider_id AS metadata_provider_id,
                m.snapshot_id AS metadata_snapshot_id,
                m.metadata_key AS metadata_metadata_key,
                m.value_type AS metadata_value_type,
                m.value_text AS metadata_value_text,
                m.value_int AS metadata_value_int,
                m.value_real AS metadata_value_real,
                m.value_datetime AS metadata_value_datetime,
                m.value_json AS metadata_value_json,
                m.confidence AS metadata_confidence,
                m.removed AS metadata_removed
            FROM assets AS f
            LEFT JOIN metadata AS m
                ON m.asset_id = f.id AND m.removed = 0
        """
        params: list[Any] = []
        clauses: list[str] = []
        if provider_id:
            clauses.append("f.provider_id = ?")
            params.append(provider_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY f.id, m.id"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
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
                    "provider_id": row["file_provider_id"],
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
        schema = {k: get_metadata_schema(k) for k in metadata_counter.keys()}
        return {"schema": schema, "stats": stats, "records": result}

    def get_metadata_for_file(
        self,
        asset_id: str,
        *,
        provider_id: str | None = None,
        snapshot_id: int | None = None,
        metadata_key: MetadataKey | None = None,
    ) -> list[Metadata]:
        """Fetch metadata rows for a single file record with optional filters."""

        query = """-- sql
            SELECT
                id,
                asset_id,
                provider_id,
                snapshot_id,
                metadata_key,
                value_type,
                value_text,
                value_int,
                value_real,
                value_datetime,
                value_json,
                confidence,
                removed
            FROM metadata
            WHERE asset_id = ?
        """
        params: list[Any] = [asset_id]
        if provider_id is not None:
            query += " AND provider_id = ?"
            params.append(provider_id)
        if snapshot_id is not None:
            query += " AND snapshot_id = ?"
            params.append(snapshot_id)
        if metadata_key is not None:
            query += " AND metadata_key = ?"
            params.append(str(metadata_key))
        query += " ORDER BY snapshot_id DESC, id DESC"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        return [Metadata.from_sql_row(dict(row)) for row in rows]

    def get_latest_metadata_for_file(
        self,
        asset_id: str,
        *,
        provider_id: str | None = None,
        metadata_key: MetadataKey | None = None,
    ) -> list[Metadata]:
        """Return the latest metadata entry per provider/value for a file."""

        provider_ids: set[str]
        if provider_id:
            provider_ids = {provider_id}
        else:
            with self._lock:
                provider_rows = self.conn.execute(
                    """-- sql
                    SELECT DISTINCT provider_id
                    FROM metadata
                    WHERE asset_id = ?
                    """,
                    (asset_id,),
                ).fetchall()
            provider_ids = {row["provider_id"] for row in provider_rows}
        if not provider_ids:
            return []

        state = self._load_current_metadata_state(asset_id, provider_ids)
        results: list[Metadata] = []
        for (state_provider, state_key), values in state.items():
            if metadata_key is not None and state_key != metadata_key:
                continue
            for payload in values.values():
                row_payload = {
                    "id": None,
                    "asset_id": asset_id,
                    "provider_id": state_provider,
                    "snapshot_id": payload.get("snapshot_id"),
                    "metadata_key": str(state_key),
                    "value_type": payload["value_type"],
                    "value_text": payload["columns"].get("value_text"),
                    "value_int": payload["columns"].get("value_int"),
                    "value_real": payload["columns"].get("value_real"),
                    "value_datetime": payload["columns"].get("value_datetime"),
                    "value_json": payload["columns"].get("value_json"),
                    "confidence": payload["confidence"],
                    "removed": int(payload["removed"]),
                }
                results.append(Metadata.from_sql_row(row_payload))

        return results

    def insert_metadata(
        self,
        entries: Iterable[Metadata],
        *,
        snapshot: Snapshot,
        default_provider_id: str | None = None,
        stats: SnapshotStats | None = None,
    ) -> int:
        """Insert metadata rows that were produced outside of a source scan."""

        source_fallback = default_provider_id or snapshot.provider_id
        if source_fallback is None:
            raise ValueError("A source id is required to insert metadata entries")
        inserted = 0
        with self._lock:
            for entry in entries:
                if not entry.asset_id:
                    raise ValueError("Metadata entry missing asset_id")
                if entry.value is None:
                    raise ValueError(
                        "Cannot insert metadata entries with value=None; use upsert_asset to clear keys"
                    )
                entry_provider_id = entry.provider_id or source_fallback
                columns = entry.as_sql_columns()
                value_json = columns["value_json"]
                if value_json is not None and not isinstance(value_json, str):
                    columns["value_json"] = json.dumps(value_json, sort_keys=True)
                self.conn.execute(
                    """-- sql
                INSERT INTO metadata (
                    asset_id,
                    provider_id,
                    snapshot_id,
                    metadata_key,
                    value_type,
                    value_text,
                    value_int,
                    value_real,
                    value_datetime,
                    value_json,
                    confidence,
                    removed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        entry.asset_id,
                        entry_provider_id,
                        snapshot.id,
                        entry.key,
                        entry.value_type,
                        columns["value_text"],
                        columns["value_int"],
                        columns["value_real"],
                        columns["value_datetime"],
                        columns["value_json"],
                        entry.confidence,
                        int(entry.removed),
                    ),
                )
                inserted += 1
            self.conn.commit()
        if stats and inserted:
            stats.record_metadata_diff(inserted, 0)
        return inserted

    def replace_relationships(
        self,
        *,
        provider_id: str,
        snapshot: Snapshot,
        relationships: Iterable[RelationshipRecord],
        stats: SnapshotStats | None = None,
    ) -> int:
        """Append relationship changes for the given snapshot."""

        if not provider_id:
            raise ValueError("provider_id is required to store relationships")
        rows = list(relationships)
        if not rows:
            return 0

        normalized: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        provider_scope: set[str] = set()
        added_relationships = 0
        removed_relationships = 0
        for rel in rows:
            rel_provider_id = rel.provider_id or provider_id
            provider_scope.add(rel_provider_id)
            identity = (
                rel_provider_id,
                rel.from_id,
                rel.to_id,
                rel.relationship_type,
            )
            description = rel.description
            if rel.attributes:
                payload = json.dumps(rel.attributes, default=str, sort_keys=True)
                description = description or payload
            normalized[identity] = {
                "provider_id": rel_provider_id,
                "from_id": rel.from_id,
                "to_id": rel.to_id,
                "relationship_type": rel.relationship_type,
                "removed": bool(rel.removed),
                "confidence": rel.confidence,
                "description": description,
            }

        existing_state = self._load_relationship_state(provider_scope)
        rows_to_insert: list[tuple[Any, ...]] = []

        for identity, payload in normalized.items():
            state = existing_state.get(identity)
            currently_active = state is not None and not state["removed"]
            desired_removed = payload["removed"]

            if desired_removed:
                if not currently_active:
                    continue
                rows_to_insert.append(
                    (
                        payload["provider_id"],
                        payload["from_id"],
                        payload["to_id"],
                        payload["relationship_type"],
                        snapshot.id,
                        1,
                        payload["confidence"],
                        payload["description"],
                    )
                )
                removed_relationships += 1
                existing_state[identity] = {"removed": True, "snapshot_id": snapshot.id}
            else:
                if currently_active:
                    continue
                rows_to_insert.append(
                    (
                        payload["provider_id"],
                        payload["from_id"],
                        payload["to_id"],
                        payload["relationship_type"],
                        snapshot.id,
                        0,
                        payload["confidence"],
                        payload["description"],
                    )
                )
                added_relationships += 1
                existing_state[identity] = {
                    "removed": False,
                    "snapshot_id": snapshot.id,
                }

        if not rows_to_insert:
            return 0

        with self._lock:
            self.conn.executemany(
                """-- sql
            INSERT INTO asset_relationships (
                provider_id,
                from_id,
                to_id,
                relationship_type,
                snapshot_id,
                removed,
                confidence,
                description
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                rows_to_insert,
            )
            self.conn.commit()
        if stats and (added_relationships or removed_relationships):
            stats.record_relationship_diff(added_relationships, removed_relationships)
        return len(rows_to_insert)

    def get_latest_metadata_by_key(
        self, metadata_key: MetadataKey
    ) -> list[tuple[str, Metadata]]:
        """Return the most recent metadata rows for active files matching the key."""

        query = """-- sql
            SELECT
                f.id AS file_id,
                f.provider_id AS file_provider_id,
                m.id AS metadata_entry_id,
                m.asset_id AS metadata_asset_id,
                m.provider_id AS metadata_provider_id,
                m.snapshot_id AS metadata_snapshot_id,
                m.metadata_key AS metadata_metadata_key,
                m.value_type AS metadata_value_type,
                m.value_text AS metadata_value_text,
                m.value_int AS metadata_value_int,
                m.value_real AS metadata_value_real,
                m.value_datetime AS metadata_value_datetime,
                m.value_json AS metadata_value_json,
                m.confidence AS metadata_confidence,
                m.removed AS metadata_removed
            FROM metadata AS m
            JOIN assets AS f
                ON m.asset_id = f.id
            WHERE f.deleted_snapshot_id IS NULL
              AND m.metadata_key = ?
                            AND m.removed = 0
              AND m.snapshot_id = (
                    SELECT MAX(m2.snapshot_id)
                    FROM metadata AS m2
                    WHERE m2.asset_id = m.asset_id
                      AND m2.provider_id = m.provider_id
                      AND m2.metadata_key = m.metadata_key
                                            AND m2.removed = 0
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
        provider_id: str | None = None,
        file_id: str | None = None,
        relationship_provider_id: str | None = None,
    ) -> list[AssetRelationship]:
        query = """-- sql
            SELECT
                r.id,
                r.provider_id,
                r.from_id,
                r.to_id,
                r.relationship_type,
                r.snapshot_id,
                r.removed,
                r.confidence,
                r.description,
            FROM asset_relationships AS r
            JOIN assets AS f_from ON f_from.id = r.from_id
            JOIN assets AS f_to ON f_to.id = r.to_id
        """
        params: list[Any] = []
        clauses: list[str] = ["r.removed = 0"]
        if provider_id:
            clauses.append("(f_from.provider_id = ? OR f_to.provider_id = ?)")
            params.extend([provider_id, provider_id])
        if file_id:
            clauses.append("(r.from_id = ? OR r.to_id = ?)")
            params.extend([file_id, file_id])
        if relationship_provider_id:
            clauses.append("r.provider_id = ?")
            params.append(relationship_provider_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY r.snapshot_id DESC, r.id DESC"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
        return [
            AssetRelationship(
                id=row["id"],
                provider_id=row["provider_id"],
                from_id=row["from_id"],
                to_id=row["to_id"],
                relationship_type=row["relationship_type"],
                snapshot_id=row["snapshot_id"],
                removed=bool(row["removed"]),
                confidence=row["confidence"],
                description=row["description"],
            )
            for row in rows
        ]

    @staticmethod
    def _metadata_from_join_row(row: sqlite3.Row) -> Metadata | None:
        entry_id = row["metadata_entry_id"]
        if entry_id is None:
            return None
        removed_value = (
            row["metadata_removed"] if "metadata_removed" in row.keys() else 0
        )
        payload = {
            "id": entry_id,
            "asset_id": row["metadata_asset_id"],
            "provider_id": row["metadata_provider_id"],
            "snapshot_id": row["metadata_snapshot_id"],
            "metadata_key": row["metadata_metadata_key"],
            "value_type": row["metadata_value_type"],
            "value_text": row["metadata_value_text"],
            "value_int": row["metadata_value_int"],
            "value_real": row["metadata_value_real"],
            "value_datetime": row["metadata_value_datetime"],
            "value_json": row["metadata_value_json"],
            "confidence": row["metadata_confidence"],
            "removed": removed_value,
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
