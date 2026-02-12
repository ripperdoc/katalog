-- name: pragma_init
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -65536;
PRAGMA busy_timeout = 5000;
PRAGMA wal_autocheckpoint = 1000;

-- name: create_actors
CREATE TABLE IF NOT EXISTS actors (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    plugin_id TEXT,
    identity_key TEXT,
    config JSON,
    config_toml TEXT,
    type INTEGER NOT NULL,
    disabled BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_actors_identity_unique
    ON actors (identity_key)
    WHERE identity_key IS NOT NULL;

-- name: create_changesets
CREATE TABLE IF NOT EXISTS changesets (
    id INTEGER PRIMARY KEY,
    message TEXT,
    running_time_ms INTEGER,
    status TEXT NOT NULL,
    data JSON
);

-- name: create_changeset_actors
CREATE TABLE IF NOT EXISTS changeset_actors (
    id INTEGER PRIMARY KEY,
    changeset_id INTEGER NOT NULL REFERENCES changesets(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    UNIQUE (changeset_id, actor_id)
);

-- name: create_assets
CREATE TABLE IF NOT EXISTS assets (
    id INTEGER PRIMARY KEY,
    canonical_asset_id INTEGER REFERENCES assets(id) ON DELETE RESTRICT,
    actor_id INTEGER REFERENCES actors(id) ON DELETE RESTRICT,
    namespace TEXT NOT NULL,
    external_id TEXT NOT NULL,
    canonical_uri TEXT NOT NULL,
    UNIQUE (namespace, external_id)
);

-- name: create_metadata_registry
CREATE TABLE IF NOT EXISTS metadata_registry (
    id INTEGER PRIMARY KEY,
    plugin_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value_type INTEGER NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    width INTEGER,
    UNIQUE (plugin_id, key)
);

-- name: create_asset_collections
CREATE TABLE IF NOT EXISTS asset_collections (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    source JSON,
    membership_key_id INTEGER REFERENCES metadata_registry(id) ON DELETE RESTRICT,
    item_count INTEGER NOT NULL DEFAULT 0,
    refresh_mode TEXT NOT NULL,
    created_at DATETIME,
    updated_at DATETIME
);

-- name: create_metadata
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    actor_id INTEGER NOT NULL REFERENCES actors(id) ON DELETE CASCADE,
    changeset_id INTEGER NOT NULL REFERENCES changesets(id) ON DELETE CASCADE,
    metadata_key_id INTEGER NOT NULL REFERENCES metadata_registry(id) ON DELETE RESTRICT,
    value_type INTEGER NOT NULL,
    value_text TEXT,
    value_int INTEGER,
    value_real REAL,
    value_datetime DATETIME,
    value_json JSON,
    value_relation_id INTEGER REFERENCES assets(id) ON DELETE CASCADE,
    value_collection_id INTEGER REFERENCES asset_collections(id) ON DELETE CASCADE,
    removed BOOLEAN NOT NULL DEFAULT 0,
    confidence REAL
);

-- name: create_metadata_indexes
CREATE INDEX IF NOT EXISTS idx_metadata_asset_key_changeset
    ON metadata (asset_id, metadata_key_id, changeset_id);
CREATE INDEX IF NOT EXISTS idx_metadata_key_collection
    ON metadata (metadata_key_id, value_collection_id);

-- name: create_asset_indexes
CREATE INDEX IF NOT EXISTS idx_asset_canonical_asset_id
    ON assets (canonical_asset_id);

-- name: create_asset_search
CREATE VIRTUAL TABLE IF NOT EXISTS asset_search
USING fts5(doc, tokenize='unicode61', detail='none');
