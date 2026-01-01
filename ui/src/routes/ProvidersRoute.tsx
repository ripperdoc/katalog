import { useCallback, useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchProviders, startScan } from "../api/client";
import type { Provider } from "../types/api";

function ProvidersRoute() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanningId, setScanningId] = useState<number | null>(null);
  const navigate = useNavigate();

  const loadProviders = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchProviders();
      setProviders(response.providers ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setProviders([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadProviders();
  }, [loadProviders]);

  const triggerScan = async (providerId?: number) => {
    setScanningId(providerId ?? 0);
    setError(null);
    try {
      const snapshot = await startScan(providerId);
      navigate(`/snapshots/${snapshot.id}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    } finally {
      setScanningId(null);
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Providers</h2>
          <p>Configured source and processor providers.</p>
        </div>
        <button type="button" onClick={() => triggerScan(undefined)} disabled={loading || scanningId !== null}>
          {scanningId === null ? "Scan all sources" : "Starting..."}
        </button>
      </header>
      {error && <p className="error">{error}</p>}
      <div className="record-list">
        {providers.map((provider) => (
          <div key={provider.id} className="file-card">
            <div className="status-bar">
              <strong>
                #{provider.id} {provider.name}
              </strong>
              <span>{provider.type}</span>
            </div>
            <p>Plugin: {provider.plugin_id ?? "n/a"}</p>
            <div className="button-row">
              <Link to={`/providers/${provider.id}`} className="link-button">
                Details
              </Link>
              <button
                type="button"
                onClick={() => triggerScan(provider.id)}
                disabled={scanningId !== null}
              >
                {scanningId === provider.id ? "Starting..." : "Scan"}
              </button>
            </div>
          </div>
        ))}
        {!loading && providers.length === 0 && <div className="empty-state">No providers found.</div>}
      </div>
    </section>
  );
}

export default ProvidersRoute;
