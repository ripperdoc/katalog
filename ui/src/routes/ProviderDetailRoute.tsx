import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { fetchProvider, startScan } from "../api/client";
import type { Provider, Snapshot } from "../types/api";

function ProviderDetailRoute() {
  const { providerId } = useParams();
  const navigate = useNavigate();
  const [provider, setProvider] = useState<Provider | null>(null);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanning, setScanning] = useState(false);

  const providerIdNum = providerId ? Number(providerId) : NaN;

  const loadProvider = useCallback(async () => {
    if (!providerIdNum || Number.isNaN(providerIdNum)) {
      setError("Invalid provider id");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const response = await fetchProvider(providerIdNum);
      setProvider(response.provider);
      setSnapshots(response.snapshots ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setProvider(null);
      setSnapshots([]);
    } finally {
      setLoading(false);
    }
  }, [providerIdNum]);

  useEffect(() => {
    void loadProvider();
  }, [loadProvider]);

  const triggerScan = async () => {
    if (!providerIdNum || Number.isNaN(providerIdNum)) {
      setError("Invalid provider id");
      return;
    }
    setScanning(true);
    setError(null);
    try {
      const snapshot = await startScan(providerIdNum);
      navigate(`/snapshots/${snapshot.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setScanning(false);
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Provider #{providerId}</h2>
          <p>Inspect provider details and snapshots.</p>
        </div>
        <div className="button-row">
          <Link to="/providers" className="link-button">
            Back
          </Link>
          <button type="button" onClick={triggerScan} disabled={scanning || loading}>
            {scanning ? "Starting..." : "Scan"}
          </button>
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      {provider && (
        <div className="record-list">
          <div className="file-card">
            <h3>Provider</h3>
            <pre>{JSON.stringify(provider, null, 2)}</pre>
          </div>
          <div className="file-card">
            <h3>Snapshots</h3>
            {snapshots.length === 0 && <div className="empty-state">No snapshots yet.</div>}
            {snapshots.map((snap) => (
              <div key={snap.id} className="status-bar">
                <Link to={`/snapshots/${snap.id}`}>Snapshot #{snap.id}</Link>
                <span>{snap.status}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {!provider && !loading && !error && <div className="empty-state">Provider not found.</div>}
    </section>
  );
}

export default ProviderDetailRoute;
