import { useCallback, useEffect, useMemo, useState } from "react";
import { HeaderObject, SimpleTable } from "simple-table-core";
import { fetchChangesets } from "../api/client";
import AppHeader from "../components/AppHeader";
import ChangesetCell from "../components/ChangesetCell";
import type { Changeset } from "../types/api";
import "simple-table-core/styles.css";

function ChangesetsRoute() {
  const [changesets, setChangesets] = useState<Changeset[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadChangesets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchChangesets();
      setChangesets(response.changesets ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setChangesets([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadChangesets();
  }, [loadChangesets]);

  const headers: HeaderObject[] = useMemo(
    () => [
      {
        accessor: "id",
        label: "ID",
        width: 100,
        type: "number",
        cellRenderer: ChangesetCell,
      },
      { accessor: "status", label: "Status", width: 140, type: "string" },
      { accessor: "actor_label", label: "Actors", width: "1fr", type: "string" },
      { accessor: "message", label: "Message", width: "1.5fr", type: "string" },
      { accessor: "running_time_ms", label: "Runtime (ms)", width: 160, type: "number" },
    ],
    [],
  );

  const rows = useMemo(
    () =>
      changesets.map((snap) => ({
        ...snap,
        actor_label: Array.isArray(snap.actor_ids) ? snap.actor_ids.join(", ") : "n/a",
      })),
    [changesets],
  );

  return (
    <>
      <AppHeader>
        <div>
          <h2>History</h2>
          <p>Changes made to the data.</p>
        </div>
        <div className="panel-actions">
          <button
            type="button"
            className="app-btn btn-primary"
            onClick={() => loadChangesets()}
            disabled={loading}
          >
            {loading ? "Loading..." : "Refresh"}
          </button>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          <div className="table-container">
            <SimpleTable
              defaultHeaders={headers}
              rows={rows}
              height="60vh"
              selectableCells={true}
              shouldPaginate={false}
              isLoading={loading}
            />
          </div>
          {!loading && rows.length === 0 && <div className="empty-state">No changesets found.</div>}
        </section>
      </main>
    </>
  );
}

export default ChangesetsRoute;
