# Database Schema Diagram

## Entity Relationship Diagram

```mermaid
erDiagram
    Actor ||--o{ Metadata : creates
    Actor ||--o{ ChangesetActor : participates
    Actor {
        int id PK
        string name UK
        string plugin_id
        json config
        text config_toml
        int type
        bool disabled
        datetime created_at
        datetime updated_at
    }

    Asset ||--o{ Metadata : has
    Asset {
        int id PK
        string external_id UK
        string canonical_uri
    }

    AssetCollection ||--o{ Metadata : referenced_by
    AssetCollection }o--|| MetadataRegistry : membership_key
    AssetCollection {
        int id PK
        string name UK
        text description
        json source
        int membership_key_id FK
        int item_count
        string refresh_mode
        datetime created_at
        datetime updated_at
    }

    Changeset ||--o{ Metadata : contains
    Changeset ||--o{ ChangesetActor : links
    Changeset {
        int id PK
        string message
        int running_time_ms
        string status
        json data
    }

    ChangesetActor }o--|| Changeset : belongs_to
    ChangesetActor }o--|| Actor : involves
    ChangesetActor {
        int id PK
        int changeset_id FK
        int actor_id FK
    }

    Metadata }o--|| Asset : belongs_to
    Metadata }o--|| Actor : created_by
    Metadata }o--|| Changeset : part_of
    Metadata }o--|| MetadataRegistry : defines_type
    Metadata }o--o| Asset : value_relation
    Metadata }o--o| AssetCollection : value_collection
    Metadata {
        int id PK
        int asset_id FK
        int actor_id FK
        int changeset_id FK
        int metadata_key_id FK
        int value_type
        text value_text
        bigint value_int
        float value_real
        datetime value_datetime
        json value_json
        int value_relation_id FK
        int value_collection_id FK
        bool removed
        float confidence
    }

    MetadataRegistry ||--o{ Metadata : type_for
    MetadataRegistry ||--o{ AssetCollection : defines_membership
    MetadataRegistry {
        int id PK
        string plugin_id
        string key
        int value_type
        string title
        text description
        int width
    }
```

## Key Relationships

- **Actor**: Represents plugins (sources, processors, analyzers, editors, exporters) that interact with assets
- **Asset**: Core entity representing a file or resource
- **Metadata**: Flexible key-value store for asset properties, versioned by changeset
- **Changeset**: Tracks changes made during operations (scans, edits)
- **MetadataRegistry**: Defines available metadata keys and their types
- **AssetCollection**: Groups of assets with shared properties
- **ChangesetActor**: Many-to-many relationship between changesets and actors

## Special Features

- Metadata uses polymorphic value columns (value_text, value_int, value_real, etc.)
- Metadata can reference other Assets (value_relation) or Collections (value_collection)
- All metadata changes are versioned through changesets for audit trail
- Soft-delete pattern via `removed` flag on Metadata
- Confidence scores on metadata for ML/AI-generated values

---

## Instructions for LLM to Regenerate This Diagram

To regenerate this database schema diagram:

1. Read all Python files in `src/katalog/models/` directory
2. Identify all classes that inherit from `tortoise.models.Model`
3. For each model class:
   - Extract the table name (class name)
   - List all field definitions using Tortoise ORM field types
   - Identify primary keys (PK), foreign keys (FK), unique constraints (UK)
   - Note relationships: ForeignKeyField, on_delete behavior
4. Create a Mermaid ERD using this format:
   ```
   EntityName {
       type field_name constraints
   }
   ```
5. Define relationships using Mermaid syntax:
   - `||--o{` for one-to-many
   - `}o--||` for many-to-one
   - `}o--o{` for many-to-many
   - `}o--o|` for optional relationships
6. Add a relationships section explaining the purpose of each entity
7. Note any special patterns (polymorphic columns, soft deletes, versioning)

Key files to analyze:
- `models/core.py`: Actor, Changeset, ChangesetActor
- `models/assets.py`: Asset, AssetCollection
- `models/metadata.py`: Metadata, MetadataRegistry
- `models/views.py`: View/display models (non-database)
