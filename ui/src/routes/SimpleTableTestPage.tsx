import React from "react";
import { SimpleTable, HeaderObject, ColumnType } from "simple-table-core";
import { createRoot } from "react-dom/client";
import "simple-table-core/styles.css";

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
  {
    accessor: "flag/starred",
    label: "Favorited",
    width: 100,
    type: "number",
    isSortable: false,
    filterable: true,
  },
  {
    accessor: "hash/md5",
    label: "MD5 Hash",
    width: 200,
    type: "string",
    isSortable: false,
    filterable: true,
  },
] as HeaderObject[];

const makeRows = (rows = 100, headers: HeaderObject[]) =>
  Array.from({ length: rows }).map((_, r) => {
    const row: Record<string, unknown> = { id: r + 1 };
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i];
      const accessor = (header as any).accessor || `col${i}`;
      const colType = ((header as any).type as ColumnType) || "string";

      if (colType === "number") {
        // produce a simple numeric value with small variance
        row[accessor] = r + 1 + i * 0.01;
      } else if (colType === "date") {
        // produce ISO date strings, decreasing by row
        const d = new Date(Date.now() - r * 24 * 60 * 60 * 1000);
        row[accessor] = d.toISOString();
      } else {
        // default string
        row[accessor] = `R${r + 1}C${i + 1}`;
      }
    }
    return row as Record<string, unknown>;
  });

const SimpleTableTestPage = () => {
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
          <SimpleTable
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
          />
        </div>
      </section>
    </main>
  );
};

const rootEl = document.getElementById("root");
if (!rootEl) throw new Error("Root element not found");
const root = createRoot(rootEl);
root.render(<SimpleTableTestPage />);
