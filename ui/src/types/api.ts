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
