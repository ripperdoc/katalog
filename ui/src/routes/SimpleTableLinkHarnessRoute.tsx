import { useMemo } from "react";
import { useLocation } from "react-router-dom";
import { CellRendererProps, ReactHeaderObject, SimpleTable } from "@simple-table/react";
import AppHeader from "../components/AppHeader";
import AppLink from "../components/AppLink";
import { simpleTableLegacyAppearance } from "../components/simpleTableAppearance";

const LinkListCell = ({ row }: CellRendererProps) => {
  const actorId = String(row["actorId"] ?? "");
  const assetId = String(row["assetId"] ?? "");
  const changesetId = String(row["changesetId"] ?? "");

  return (
    <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
      <AppLink to={`/simple-table-link-harness/actor/${actorId}`}>Actor {actorId}</AppLink>
      <AppLink to={`/simple-table-link-harness/asset/${assetId}`}>Asset {assetId}</AppLink>
      <AppLink to={`/simple-table-link-harness/changeset/${changesetId}`}>
        Changeset {changesetId}
      </AppLink>
    </div>
  );
};

function SimpleTableLinkHarnessRoute() {
  const location = useLocation();

  const headers = useMemo<ReactHeaderObject[]>(
    () => [
      {
        accessor: "links",
        label: "Links",
        width: "2fr",
        type: "string",
        cellRenderer: LinkListCell,
      },
      {
        accessor: "label",
        label: "Label",
        width: "1fr",
        type: "string",
      },
    ],
    [],
  );

  const rows = useMemo(
    () => [
      {
        id: "row-1",
        links: "row-1-links",
        label: "First row",
        actorId: 101,
        assetId: 201,
        changesetId: 301,
      },
      {
        id: "row-2",
        links: "row-2-links",
        label: "Second row",
        actorId: 102,
        assetId: 202,
        changesetId: 302,
      },
    ],
    [],
  );

  return (
    <>
      <AppHeader breadcrumbLabel="SimpleTable Link Harness" />
      <main className="app-main">
        <section className="panel">
          <p className="note">Current path: {location.pathname}</p>
          <div className="table-container">
            <SimpleTable
              {...simpleTableLegacyAppearance}
              defaultHeaders={headers}
              rows={rows}
              height="40vh"
              selectableCells={true}
              shouldPaginate={false}
              getRowId={({ row }) => String(row["id"] ?? "")}
            />
          </div>
        </section>
      </main>
    </>
  );
}

export default SimpleTableLinkHarnessRoute;
