import { Link } from "react-router-dom";
import type { CellRendererProps } from "simple-table-core";

function ChangesetCell({ value }: CellRendererProps) {
  const changesetId = typeof value === "number" ? value : Number(value);

  if (!changesetId || Number.isNaN(changesetId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return <Link to={`/changesets/${changesetId}`}>{String(changesetId)}</Link>;
}

export default ChangesetCell;
