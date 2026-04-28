import { useState } from "react";
import { SimpleTable, ReactHeaderObject, ColumnType } from "@simple-table/react";
import { createRoot } from "react-dom/client";
import "@simple-table/react/styles.css";
import { simpleTableLegacyAppearance } from "../components/simpleTableAppearance";

const headers = [
  {
    accessor: "asset/id",
    label: "Asset ID",
    width: 200,
    type: "number",
    isSortable: true,
    filterable: false,
  },
  {
    accessor: "asset/external_id",
    label: "External ID",
    width: 200,
    type: "string",
    isSortable: false,
    filterable: false,
  },
  {
    accessor: "file/path",
    label: "Path",
    width: 200,
    type: "string",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "file/filename",
    label: "Filename",
    width: 200,
    type: "string",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "file/size",
    label: "Size (bytes)",
    width: 120,
    type: "number",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "file/type",
    label: "MIME Type",
    width: 200,
    type: "string",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "time/created",
    label: "Created",
    width: 200,
    type: "date",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "time/modified",
    label: "Modified",
    width: 200,
    type: "date",
    isSortable: false,
    filterable: true,
  },
] as ReactHeaderObject[];

const makeRows = (rows = 100, tableHeaders: ReactHeaderObject[]) =>
  Array.from({ length: rows }).map((_, r) => {
    const row: Record<string, unknown> = { id: r + 1 };
    for (let i = 0; i < tableHeaders.length; i++) {
      const header = tableHeaders[i];
      const accessor = (header as { accessor?: string }).accessor ?? `col${i}`;
      const colType = ((header as { type?: ColumnType }).type as ColumnType) ?? "string";
      if (colType === "number") {
        row[accessor] = r + 1 + i * 0.01;
      } else if (colType === "date") {
        const d = new Date(Date.now() - r * 24 * 60 * 60 * 1000);
        row[accessor] = d.toISOString();
      } else {
        row[accessor] = `R${r + 1}C${i + 1}`;
      }
    }
    return row;
  });

function SimpleTableTestPage() {
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set());
  const records = makeRows(100, headers);
  const total = 1000;
  const pagination = {
    limit: 50,
    offset: 0,
  };

  return (
    <main className="zapp-main">
      <section className="zpanel">
        <div className="ztable-container">
          <div style={{ marginBottom: "8px" }}>
            Selected rows: {selectedRows.size} | Raw IDs: {[...selectedRows].join(", ") || "none"}
          </div>
          <SimpleTable
            {...simpleTableLegacyAppearance}
            defaultHeaders={headers}
            rows={records}
            height="100vh"
            autoExpandColumns={false}
            selectableCells={true}
            columnResizing={true}
            shouldPaginate={true}
            rowsPerPage={pagination.limit}
            serverSidePagination={true}
            enableRowSelection={true}
            totalRowCount={total ?? records.length}
            onRowSelectionChange={({ selectedRows: nextSelectedRows }) => {
              setSelectedRows(nextSelectedRows);
            }}
          />
        </div>
      </section>
    </main>
  );
}

const rootEl = document.getElementById("root");
if (!rootEl) throw new Error("Root element not found");
const root = createRoot(rootEl);
root.render(<SimpleTableTestPage />);
