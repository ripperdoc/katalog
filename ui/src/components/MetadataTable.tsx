import { useMemo, useState } from "react";
import { SimpleTable, HeaderObject } from "simple-table-core";
import type { MetadataRecord } from "../types/api";
import { groupByNested } from "../utils/metadataGrouping";
import ProviderCell from "./ProviderCell";
import SnapshotCell from "./SnapshotCell";

type MetadataTableProps = {
  metadata: MetadataRecord[];
  initialView?: "flat" | "providerGrouped";
};

const flatHeaders: HeaderObject[] = [
  // { accessor: "id", label: "ID", width: 80, type: "number" },
  // { accessor: "asset_id", label: "Asset", width: 80, type: "number" },
  {
    accessor: "provider_id",
    label: "Provider",
    width: "1fr",
    type: "number",
    cellRenderer: ProviderCell,
  },
  {
    accessor: "snapshot_id",
    label: "Snapshot",
    width: "1fr",
    type: "number",
    cellRenderer: SnapshotCell,
  },
  { accessor: "key", label: "Key", width: "1.4fr", type: "string" },
  // { accessor: "value_type", label: "Type", width: 100, type: "string" },
  { accessor: "value", label: "Value", width: "2fr", type: "string" },
  { accessor: "removed", label: "Removed", width: 100, type: "string" },
  // { accessor: "confidence", label: "Conf", width: 100, type: "number" },
];

const providerGroupedHeaders: HeaderObject[] = [
  // { accessor: "id", label: "ID", width: 80, type: "number" },
  // { accessor: "asset_id", label: "Asset", width: 80, type: "number" },
  {
    accessor: "provider_id",
    label: "Provider",
    width: "1fr",
    type: "number",
    expandable: true,
    cellRenderer: ProviderCell,
  },
  {
    accessor: "snapshot_id",
    label: "Snapshot",
    width: "1fr",
    type: "number",
    cellRenderer: SnapshotCell,
  },
  { accessor: "key", label: "Key", width: "1.4fr", type: "string" },
  // { accessor: "value_type", label: "Type", width: 100, type: "string" },
  { accessor: "value", label: "Value", width: "2fr", type: "string" },
  { accessor: "removed", label: "Removed", width: 100, type: "string" },
  // { accessor: "confidence", label: "Conf", width: 100, type: "number" },
];

const viewConfigs = {
  flat: {
    headers: flatHeaders,
    rowGrouping: undefined,
  },
  providerGrouped: {
    headers: providerGroupedHeaders,
    rowGrouping: ["provider_id_children"],
  },
} satisfies Record<
  NonNullable<MetadataTableProps["initialView"]>,
  { headers: HeaderObject[]; rowGrouping: string[] | undefined }
>;

function MetadataTable({ metadata, initialView = "flat" }: MetadataTableProps) {
  const [view, setView] = useState<"flat" | "providerGrouped">(initialView);
  const rows = useMemo(() => {
    const normalized = metadata.map((m) => ({
      ...m,
      removed: m.removed ? "yes" : "no",
      value: typeof m.value === "object" ? JSON.stringify(m.value) : m.value,
    }));

    if (view === "flat") {
      return normalized;
    }

    if (view === "providerGrouped") {
      return groupByNested(normalized, ["provider_id"]);
    }

    return [];
  }, [metadata, view]);

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
          className={`button-toggle ${view === "providerGrouped" ? "is-active" : ""}`}
          aria-pressed={view === "providerGrouped"}
          onClick={() => setView("providerGrouped")}
        >
          Group by provider
        </button>
      </div>
      <SimpleTable
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
