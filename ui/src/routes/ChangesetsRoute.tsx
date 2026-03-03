import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { HeaderObject, SimpleTable } from "simple-table-core";
import { fetchChangesets } from "../api/client";
import AppHeader from "../components/AppHeader";
import ChangesetCell from "../components/ChangesetCell";
import type { Changeset } from "../types/api";
import "simple-table-core/styles.css";

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

  useEffect(() => {
    setSelectedRange(null);
  }, [rows]);

  const handleRowSelectionChange = useCallback(
    ({ selectedRows }: { selectedRows: Set<string> }) => {
      if (!selectedRows || selectedRows.size === 0) {
        setSelectedRange(null);
        return;
      }

      // simple-table-core row selection emits internal row ids (e.g. "5-177245..."),
      // where the custom getRowId() value is the last segment.
      const selectedChangesetIds = [...selectedRows]
        .map((rawRowId) => {
          const suffix = String(rawRowId).split("-").pop();
          return Number(suffix);
        })
        .filter((id) => Number.isFinite(id));

      if (selectedChangesetIds.length === 0) {
        setSelectedRange(null);
        return;
      }

      const selectedIdSet = new Set(selectedChangesetIds);
      const selectedIndices = rows
        .map((row, idx) => ({ idx, id: Number(row.id) }))
        .filter(({ id }) => selectedIdSet.has(id))
        .map(({ idx }) => idx)
        .filter((idx) => idx >= 0)
        .sort((a, b) => a - b);

      if (selectedIndices.length === 0) {
        setSelectedRange(null);
        return;
      }

      // Normalize to a contiguous range in table order.
      const firstIndex = selectedIndices[0];
      const lastIndex = selectedIndices[selectedIndices.length - 1];
      const rangeRows = rows.slice(firstIndex, lastIndex + 1);
      const rangeIds = rangeRows
        .map((row) => Number(row.id))
        .filter((id) => Number.isFinite(id));

      if (rangeIds.length === 0) {
        setSelectedRange(null);
        return;
      }

      setSelectedRange({
        from: Math.min(...rangeIds),
        to: Math.max(...rangeIds),
        selectedCount: selectedIdSet.size,
        normalizedCount: rangeIds.length,
      });
    },
    [rows],
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
                  Using contiguous range ({selectedRange.normalizedCount} rows) between selected endpoints.
                </div>
              )}
            </div>
          )}
          <div className="table-container">
            <SimpleTable
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
