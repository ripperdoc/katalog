import type { CellRendererProps } from "@simple-table/react";
import type { Actor } from "../types/api";
import { useRegistry } from "../utils/registry";
import AppLink from "./AppLink";

type ActorCellProps = CellRendererProps & {
  actorsById?: Record<number, Actor>;
};

export function ActorCellPure({ value, actorsById }: ActorCellProps) {
  const actorId = typeof value === "number" ? value : Number(value);
  const actorName = actorsById?.[actorId]?.name;

  if (!actorId || Number.isNaN(actorId)) {
    return <span>{actorName ?? String(value ?? "")}</span>;
  }

  return <AppLink to={`/actors/${actorId}`}>{actorName ?? String(actorId)}</AppLink>;
}

function ActorCell(props: CellRendererProps) {
  const { data } = useRegistry();
  return <ActorCellPure {...props} actorsById={data?.actorsById} />;
}

export default ActorCell;
