import { useMemo, useState } from "react";
import { CellRendererProps, SimpleTable, ReactHeaderObject } from "@simple-table/react";
import type { MetadataRecord } from "../types/api";
import { groupByNested } from "../utils/metadataGrouping";
import { ActorCellPure } from "./ActorCell";
import ChangesetCell from "./ChangesetCell";
import { simpleTableLegacyAppearance } from "./simpleTableAppearance";
import { useRegistry } from "../utils/registry";

type MetadataTableProps = {
  metadata: MetadataRecord[];
  initialView?: "flat" | "actorGrouped";
};

function MetadataTable({ metadata, initialView = "flat" }: MetadataTableProps) {
  const [view, setView] = useState<"flat" | "actorGrouped">(initialView);
  const { data: registryData } = useRegistry();
  const rows = useMemo(() => {
    const normalized = metadata.map((m) => ({
      ...m,
      removed: m.removed ? "yes" : "no",
      value: typeof m.value === "object" ? JSON.stringify(m.value) : m.value,
    }));

    if (view === "flat") {
      return normalized;
    }

    if (view === "actorGrouped") {
      return groupByNested(normalized, ["actor_id"]);
    }

    return [];
  }, [metadata, view]);

  const viewConfigs = useMemo(
    () => ({
      flat: {
        headers: [
          {
            accessor: "actor_id",
            label: "Actor",
            width: "1fr",
            type: "number",
            cellRenderer: (props: CellRendererProps) => (
              <ActorCellPure {...props} actorsById={registryData?.actorsById} />
            ),
          },
          {
            accessor: "changeset_id",
            label: "Changeset",
            width: "1fr",
            type: "number",
            cellRenderer: ChangesetCell,
          },
          { accessor: "key", label: "Key", width: "1.4fr", type: "string" },
          { accessor: "value", label: "Value", width: "2fr", type: "string" },
          { accessor: "removed", label: "Removed", width: 100, type: "string" },
        ] as ReactHeaderObject[],
        rowGrouping: undefined,
      },
      actorGrouped: {
        headers: [
          {
            accessor: "actor_id",
            label: "Actor",
            width: "1fr",
            type: "number",
            expandable: true,
            cellRenderer: (props: CellRendererProps) => (
              <ActorCellPure {...props} actorsById={registryData?.actorsById} />
            ),
          },
          {
            accessor: "changeset_id",
            label: "Changeset",
            width: "1fr",
            type: "number",
            cellRenderer: ChangesetCell,
          },
          { accessor: "key", label: "Key", width: "1.4fr", type: "string" },
          { accessor: "value", label: "Value", width: "2fr", type: "string" },
          { accessor: "removed", label: "Removed", width: 100, type: "string" },
        ] as ReactHeaderObject[],
        rowGrouping: ["actor_id_children"],
      },
    }),
    [registryData],
  );

  const { headers, rowGrouping } = viewConfigs[view];

  return (
    <div className="table-container">
      <div className="table-toolbar">
        <button
          type="button"
          className={`button-toggle ${view === "flat" ? "is-active" : ""}`}
          aria-pressed={view === "flat"}
          onClick={() => setView("flat")}
        >
          Flat
        </button>
        <button
          type="button"
          className={`button-toggle ${view === "actorGrouped" ? "is-active" : ""}`}
          aria-pressed={view === "actorGrouped"}
          onClick={() => setView("actorGrouped")}
        >
          Group by actor
        </button>
      </div>
      <SimpleTable
        {...simpleTableLegacyAppearance}
        defaultHeaders={headers}
        rows={rows}
        columnResizing={true}
        rowGrouping={rowGrouping}
        // height="60vh"
        selectableCells={true}
        shouldPaginate={false}
      />
      {/* <div className="file-card">
        <h3>Asset JSON</h3>
        <pre>{JSON.stringify(rows, null, 2)}</pre>
      </div> */}
    </div>
  );
}

export default MetadataTable;
