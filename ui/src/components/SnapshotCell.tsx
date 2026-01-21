import { Link } from "react-router-dom";
import type { CellRendererProps } from "simple-table-core";

function SnapshotCell({ value }: CellRendererProps) {
  const snapshotId = typeof value === "number" ? value : Number(value);

  if (!snapshotId || Number.isNaN(snapshotId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return <Link to={`/snapshots/${snapshotId}`}>{String(snapshotId)}</Link>;
}

export default SnapshotCell;
