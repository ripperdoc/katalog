import type { Row } from "simple-table-core";

export type MetadataValue = string | number | boolean | null | Record<string, unknown> | unknown[];

export interface MetadataValueEntry {
  value: MetadataValue;
  count: number;
}

export interface ColumnDefinition {
  id: string;
  value_type: number;
  registry_id: number | null;
  title: string;
  description: string;
  width: number | null;
  hidden?: boolean;
  sortable: boolean;
  filterable: boolean;
  searchable: boolean;
  plugin_id: string | null;
  key: string;
}

export interface ViewSpec {
  id: string;
  name: string;
  columns: ColumnDefinition[];
  default_sort: [string, string][];
  default_columns: string[] | null;
}

export interface ViewListResponse {
  views: ViewSpec[];
}

export interface ViewResponse {
  view: ViewSpec;
}

export interface Asset extends Row {
  [key: string]: MetadataValue | number | boolean | null | MetadataValueEntry | undefined;
}

export interface ViewAssetsResponse {
  items: Asset[];
  schema: ColumnDefinition[];
  stats: {
    returned: number;
    total: number | null;
    duration_ms?: number | null;
    duration_assets_ms?: number | null;
    duration_metadata_ms?: number | null;
    duration_count_ms?: number | null;
  };
  pagination: {
    offset: number;
    limit: number;
  };
}

export interface Actor {
  id: number;
  name: string;
  type: string;
  plugin_id: string | null;
  config: Record<string, unknown> | null;
  config_toml: string | null;
  disabled?: boolean;
  created_at: string | null;
  updated_at: string | null;
}

export interface PluginSpec {
  plugin_id: string;
  actor_type: string;
  title: string;
  description: string | null;
  origin: string;
  version: string | null;
}

export interface PluginListResponse {
  plugins: PluginSpec[];
}

export type ChangesetStatus =
  | "in_progress"
  | "partial"
  | "completed"
  | "canceled"
  | "skipped"
  | "error";

export interface Changeset {
  id: number;
  actor_ids: number[] | null;
  message: string | null;
  running_time_ms: number | null;
  status: ChangesetStatus;
  data: Record<string, unknown> | null;
  running?: boolean;
}

export interface ActorListResponse {
  actors: Actor[];
}

export interface ActorResponse {
  actor: Actor;
  changesets: Changeset[];
}

export interface ActorCreateResponse {
  actor: Actor;
}

export interface ActorUpdateResponse {
  actor: Actor;
}

export interface ChangesetListResponse {
  changesets: Changeset[];
}

export interface ChangesetResponse {
  changeset: Changeset;
  logs: ChangesetEvent[];
  running: boolean;
}

export interface ChangesetEvent {
  event: string;
  changeset_id: number;
  ts: string;
  payload: Record<string, unknown>;
}

export interface DeleteChangesetResponse {
  status: string;
  changeset_id: number;
}

export interface CancelChangesetResponse {
  status: string;
  changeset?: Changeset;
}

export interface AssetCollection {
  id: number;
  name: string;
  description: string | null;
  asset_count: number;
  source: Record<string, unknown> | null;
  refresh_mode: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface CollectionListResponse {
  collections: AssetCollection[];
}

export interface CollectionResponse {
  collection: AssetCollection;
}

export interface CollectionUpdateResponse {
  collection: AssetCollection;
}

export interface DeleteCollectionResponse {
  status: string;
  collection_id: number;
}

export interface CollectionRemoveAssetsResponse {
  removed: number;
  skipped: number;
}

export interface AssetDetailRecord {
  id: number;
  actor_id: number;
  external_id: string;
  canonical_uri: string;
}

export interface MetadataRecord {
  id: number;
  asset_id: number;
  actor_id: number;
  changeset_id: number;
  metadata_key_id: number;
  key: string;
  value_type: string;
  value: MetadataValue;
  removed: boolean;
  confidence: number | null;
}

export interface MetadataRegistryEntry {
  plugin_id: string;
  key: string;
  registry_id: number | null;
  value_type: string;
  title: string;
  description: string;
  width: number | null;
}

export interface MetadataRegistryResponse {
  registry: Record<number, MetadataRegistryEntry>;
}

export interface AssetDetailResponse {
  asset: AssetDetailRecord;
  metadata: MetadataRecord[];
}

export interface EditableMetadataSchemaResponse {
  schema: Record<string, unknown>;
  uiSchema: Record<string, unknown>;
}

export interface ChangesetChangeRecord {
  id: number;
  asset_id: number;
  actor_id: number;
  changeset_id: number;
  metadata_key: string;
  metadata_key_id: number;
  value_type: number;
  value: MetadataValue;
  removed: boolean;
}

export interface ChangesetChangesResponse {
  items: ChangesetChangeRecord[];
  stats: {
    returned: number;
    total: number | null;
    duration_ms?: number | null;
    duration_rows_ms?: number | null;
    duration_count_ms?: number | null;
  };
  pagination: {
    offset: number;
    limit: number;
  };
}
