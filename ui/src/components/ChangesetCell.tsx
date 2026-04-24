import type { CellRendererProps } from "@simple-table/react";
import AppLink from "./AppLink";

function ChangesetCell({ value }: CellRendererProps) {
  const changesetId = typeof value === "number" ? value : Number(value);

  if (!changesetId || Number.isNaN(changesetId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return <AppLink to={`/changesets/${changesetId}`}>{String(changesetId)}</AppLink>;
}

export default ChangesetCell;
