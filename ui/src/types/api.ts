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

export interface Provider {
  id: number;
  name: string;
  type: string;
  plugin_id: string | null;
  config: Record<string, unknown> | null;
  config_toml: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface PluginSpec {
  plugin_id: string;
  type: string;
  title: string;
  description: string | null;
  origin: string;
  version: string | null;
}

export interface PluginListResponse {
  plugins: PluginSpec[];
}

export type SnapshotStatus =
  | "in_progress"
  | "partial"
  | "completed"
  | "canceled"
  | "skipped"
  | "error";

export interface Snapshot {
  id: number;
  provider_id: number | null;
  provider_name: string | null;
  note: string | null;
  started_at: string | null;
  completed_at: string | null;
  status: SnapshotStatus;
  metadata: Record<string, unknown> | null;
}

export interface ProviderListResponse {
  providers: Provider[];
}

export interface ProviderResponse {
  provider: Provider;
  snapshots: Snapshot[];
}

export interface ProviderCreateResponse {
  provider: Provider;
}

export interface ProviderUpdateResponse {
  provider: Provider;
}

export interface SnapshotListResponse {
  snapshots: Snapshot[];
}

export interface SnapshotResponse {
  snapshot: Snapshot;
  logs: string[];
  running: boolean;
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

export interface AssetDetailRecord {
  id: number;
  provider_id: number;
  external_id: string;
  canonical_uri: string;
}

export interface MetadataRecord {
  id: number;
  asset_id: number;
  provider_id: number;
  snapshot_id: number;
  metadata_key_id: number;
  key: string;
  value_type: string;
  value: MetadataValue;
  removed: boolean;
  confidence: number | null;
}

export interface AssetDetailResponse {
  asset: AssetDetailRecord;
  metadata: MetadataRecord[];
}
