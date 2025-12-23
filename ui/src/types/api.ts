import type { Row } from "simple-table-core";

export type MetadataValue = string | number | boolean | null;

export type MetadataFlat = Record<string, MetadataValue>;

export interface MetadataEntry {
  id: number;
  asset_id: string;
  provider_id: string;
  snapshot_id: number;
  plugin_id: string;
  metadata_key: string;
  value: MetadataValue;
  confidence: number;
}

export interface MetadataDefinition {
  key: string;
  value_type: string;
  title: string;
  description: string;
  width: number | null;
}

export interface Asset extends Row {
  id: string;
  provider_id: string;
  canonical_uri: string;
  created_snapshot_id: number;
  last_snapshot_id: number;
  deleted_snapshot_id: number | null;
  metadata: MetadataFlat;
}

export interface AssetResponse {
  records: Asset[];
  schema: Record<string, MetadataDefinition>;
  stats: {
    records: number;
    metadata: Record<string, number>;
  };
}

export interface AssetComplete extends Omit<Asset, "metadata"> {
  metadata: MetadataEntry[];
}

export type ViewMode = "flat" | "complete";
