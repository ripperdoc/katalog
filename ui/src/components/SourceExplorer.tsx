import { FormEvent, useCallback, useMemo, useState } from "react";
import { fetchFilesBySource } from "../api/client";
import FileTable from "./FileTable";
import type { FileRecordResponse, ViewMode } from "../types/api";

function SourceExplorer() {
  const [sourceId, setSourceId] = useState("");
  const [view, setView] = useState<ViewMode>("flat");
  const [files, setFiles] = useState<FileRecordResponse[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  const handleSubmit = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      setError(null);
      setLoading(true);
      try {
        const data = await fetchFilesBySource(sourceId.trim(), view);
        setFiles(data);
        setLastUpdated(new Date().toLocaleTimeString());
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        setFiles([]);
      } finally {
        setLoading(false);
      }
    },
    [sourceId, view]
  );

  const disabled = !sourceId.trim() || loading;

  const stats = useMemo(() => {
    if (!files.length) {
      return "No files loaded";
    }
    return `${files.length} file${files.length === 1 ? "" : "s"}`;
  }, [files]);

  return (
    <section className="panel">
      <form onSubmit={handleSubmit}>
        <label>
          Source ID
          <input
            placeholder="e.g. local_fs"
            value={sourceId}
            onChange={(event) => setSourceId(event.target.value)}
            autoComplete="off"
          />
        </label>
        <label>
          View Mode
          <select value={view} onChange={(event) => setView(event.target.value as ViewMode)}>
            <option value="flat">Flat (deduplicated metadata)</option>
            <option value="complete">Complete (per-entry metadata)</option>
          </select>
        </label>
        <div style={{ alignSelf: "end" }}>
          <button type="submit" disabled={disabled}>
            {loading ? "Loading..." : "Fetch Files"}
          </button>
        </div>
      </form>
      <div className="status-bar">
        <span>{stats}</span>
        <span>{lastUpdated ? `Updated ${lastUpdated}` : ""}</span>
      </div>
      {error && <p className="error">{error}</p>}
      {files.length === 0 && !error ? (
        <div className="empty-state">Provide a source id and run a snapshot to see files.</div>
      ) : (
        <FileTable files={files} view={view} />
      )}
    </section>
  );
}

export default SourceExplorer;
