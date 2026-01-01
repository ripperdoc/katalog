import type { Row } from "simple-table-core";

export type MetadataValue = string | number | boolean | null | Record<string, unknown> | unknown[];

export interface MetadataValueEntry {
  value: MetadataValue;
  count: number;
}

export interface MetadataDefinition {
  plugin_id: string;
  key: string;
  registry_id: number | null;
  value_type: number;
  title: string;
  description: string;
  width: number | null;
}

export interface Asset extends Row {
  id: number;
  canonical_id: string;
  canonical_uri: string;
  created: number;
  seen: number;
  deleted: number | null;
  metadata: Record<string, MetadataValueEntry>;
}

export interface AssetResponse {
  assets: Asset[];
  schema: Record<string, MetadataDefinition>;
  stats: {
    assets: number;
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
