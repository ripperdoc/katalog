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
  created_at: string | null;
  updated_at: string | null;
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

export interface SnapshotListResponse {
  snapshots: Snapshot[];
}

export interface SnapshotResponse {
  snapshot: Snapshot;
  logs: string[];
  running: boolean;
}
