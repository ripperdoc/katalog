import { Link } from "react-router-dom";
import type { CellRendererProps } from "simple-table-core";
import { useRegistry } from "../utils/registry";

function ActorCell({ value }: CellRendererProps) {
  const { data } = useRegistry();
  const actorId = typeof value === "number" ? value : Number(value);
  const actorName = data?.actorsById?.[actorId]?.name;

  if (!actorId || Number.isNaN(actorId)) {
    return <span>{actorName ?? String(value ?? "")}</span>;
  }

  return <Link to={`/actors/${actorId}`}>{actorName ?? String(actorId)}</Link>;
}

export default ActorCell;
