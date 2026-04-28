import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ReactHeaderObject, SimpleTable } from "@simple-table/react";
import { fetchChangesets } from "../api/client";
import AppHeader from "../components/AppHeader";
import ChangesetCell from "../components/ChangesetCell";
import { simpleTableLegacyAppearance } from "../components/simpleTableAppearance";
import type { Changeset } from "../types/api";
import "@simple-table/react/styles.css";

type SelectedRange = {
  from: number;
  to: number;
  selectedCount: number;
  normalizedCount: number;
};

function ChangesetsRoute() {
  const navigate = useNavigate();
  const [changesets, setChangesets] = useState<Changeset[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedRange, setSelectedRange] = useState<SelectedRange | null>(null);

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

  const headers: ReactHeaderObject[] = useMemo(
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
  const rowsRef = useRef(rows);
  useEffect(() => {
    rowsRef.current = rows;
  }, [rows]);

  useEffect(() => {
    setSelectedRange(null);
  }, [rows]);

  const handleRowSelectionChange = useCallback(
    ({ selectedRows }: { selectedRows: Set<string> }) => {
      if (!selectedRows || selectedRows.size === 0) {
        setSelectedRange(null);
        return;
      }

      const selectedIndices = [...selectedRows]
        .map((rawRowId) => Number(rawRowId))
        .filter((idx) => Number.isFinite(idx) && idx >= 0)
        .sort((a, b) => a - b);

      if (selectedIndices.length === 0) {
        setSelectedRange(null);
        return;
      }

      const firstIndex = selectedIndices[0];
      const lastIndex = selectedIndices[selectedIndices.length - 1];
      const rangeRows = rowsRef.current.slice(firstIndex, lastIndex + 1);
      const rangeIds = rangeRows.map((row) => Number(row.id)).filter((id) => Number.isFinite(id));

      if (rangeIds.length === 0) {
        setSelectedRange(null);
        return;
      }

      setSelectedRange({
        from: Math.min(...rangeIds),
        to: Math.max(...rangeIds),
        selectedCount: selectedIndices.length,
        normalizedCount: rangeIds.length,
      });
    },
    [],
  );

  const openSelectedRange = useCallback(() => {
    if (!selectedRange || selectedRange.selectedCount < 2) {
      return;
    }
    navigate(`/changesets/${selectedRange.from}..${selectedRange.to}`);
  }, [navigate, selectedRange]);

  return (
    <>
      <AppHeader />
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {selectedRange && selectedRange.selectedCount >= 2 && (
            <div className="table-toolbar" style={{ marginBottom: "0.5rem" }}>
              <button type="button" className="app-btn" onClick={openSelectedRange}>
                View changeset range {selectedRange.from}..{selectedRange.to}
              </button>
              {selectedRange.normalizedCount !== selectedRange.selectedCount && (
                <div className="note">
                  Using contiguous range ({selectedRange.normalizedCount} rows) between selected
                  endpoints.
                </div>
              )}
            </div>
          )}
          <div className="table-container">
            <SimpleTable
              {...simpleTableLegacyAppearance}
              defaultHeaders={headers}
              rows={rows}
              height="60vh"
              selectableCells={true}
              enableRowSelection={true}
              getRowId={({ row }) => String(row["id"] ?? "")}
              onRowSelectionChange={handleRowSelectionChange}
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
