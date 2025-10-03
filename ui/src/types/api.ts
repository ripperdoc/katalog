export type MetadataValue =
  | string
  | number
  | boolean
  | null
  | Record<string, unknown>
  | MetadataValue[];

export type MetadataFlat = Record<string, MetadataValue>;

export interface MetadataEntry {
  id: number;
  file_record_id: string;
  source_id: string;
  snapshot_id: number;
  plugin_id: string;
  metadata_key: string;
  value: MetadataValue;
  confidence: number;
}

export interface FileRecordFlat {
  id: string;
  source_id: string;
  canonical_uri: string;
  created_snapshot_id: number;
  last_snapshot_id: number;
  deleted_snapshot_id: number | null;
  metadata: MetadataFlat;
}

export interface FileRecordComplete extends Omit<FileRecordFlat, "metadata"> {
  metadata: MetadataEntry[];
}

export type ViewMode = "flat" | "complete";
export type FileRecordResponse = FileRecordFlat | FileRecordComplete;
