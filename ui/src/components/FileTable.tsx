import type { Asset } from "../types/api";

type Props = {
  assets: Asset[];
};

function FileTable({ assets }: Props) {
  if (!assets.length) {
    return null;
  }

  return (
    <div className="file-list">
      {assets.map((asset) => (
        <article key={asset.id} className="file-card">
          <h3>{asset.canonical_uri}</h3>
          <p>
            <strong>File ID:</strong> {asset.id}
          </p>
          <p>
            <strong>Snapshot:</strong> {asset.seen} (created {asset.created})
          </p>
          {asset.deleted !== null && (
            <p>
              <strong>Deleted Snapshot:</strong> {asset.deleted}
            </p>
          )}
          <pre>{JSON.stringify(asset.metadata, null, 2)}</pre>
        </article>
      ))}
    </div>
  );
}

export default FileTable;
