import type { CellRendererProps } from "@simple-table/react";
import AppLink from "./AppLink";
import { formatRelativeTime } from "../utils/relativeTime";

function ChangesetCell({ value }: CellRendererProps) {
  const changesetId = typeof value === "number" ? value : Number(value);

  if (!changesetId || Number.isNaN(changesetId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return (
    <AppLink to={`/changesets/${changesetId}`}>
      {formatRelativeTime(changesetId)}
    </AppLink>
  );
}

export default ChangesetCell;
