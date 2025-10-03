import type { FileRecordResponse, MetadataEntry, MetadataFlat, ViewMode } from "../types/api";

type Props = {
  files: FileRecordResponse[];
  view: ViewMode;
};

function formatMetadataFlat(metadata: MetadataFlat) {
  return JSON.stringify(metadata, null, 2);
}

function formatMetadataEntries(entries: MetadataEntry[]) {
  return JSON.stringify(entries, null, 2);
}

function FileTable({ files, view }: Props) {
  if (!files.length) {
    return null;
  }

  return (
    <div className="file-list">
      {files.map((file) => (
        <article key={file.id} className="file-card">
          <h3>{file.canonical_uri}</h3>
          <p>
            <strong>File ID:</strong> {file.id}
          </p>
          <p>
            <strong>Snapshot:</strong> {file.last_snapshot_id} (created {file.created_snapshot_id})
          </p>
          {file.deleted_snapshot_id && (
            <p>
              <strong>Deleted Snapshot:</strong> {file.deleted_snapshot_id}
            </p>
          )}
          <pre>
            {view === "flat"
              ? formatMetadataFlat(file.metadata as MetadataFlat)
              : formatMetadataEntries(file.metadata as MetadataEntry[])}
          </pre>
        </article>
      ))}
    </div>
  );
}

export default FileTable;
