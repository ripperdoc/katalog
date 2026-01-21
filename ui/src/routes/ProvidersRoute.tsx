import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import {
  fetchPlugins,
  fetchProviders,
  startScan,
  runAllProcessors,
  runAllAnalyzers,
} from "../api/client";
import type { Provider, PluginSpec } from "../types/api";

function ProvidersRoute() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanningId, setScanningId] = useState<number | null>(null);
  const navigate = useNavigate();
  const didInit = useRef(false);

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

  const loadPlugins = useCallback(async () => {
    try {
      const response = await fetchPlugins();
      setPlugins(response.plugins ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setPlugins([]);
    }
  }, []);

  useEffect(() => {
    if (didInit.current) {
      return;
    }
    didInit.current = true;
    void loadProviders();
    void loadPlugins();
  }, [loadProviders, loadPlugins]);

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

  const filteredPlugins = useCallback(
    (ptype: "SOURCE" | "PROCESSOR" | "ANALYZER") => plugins.filter((p) => p.type === ptype),
    [plugins],
  );

  const grouped = {
    sources: providers.filter((p) => p.type === "SOURCE"),
    processors: providers.filter((p) => p.type === "PROCESSOR"),
    analyzers: providers.filter((p) => p.type === "ANALYZER"),
  };

  return (
    <>
      <AppHeader>
        <div>
          <h2>Providers</h2>
          <p>Configured source, processor, and analyzer providers.</p>
        </div>
      </AppHeader>
      <main className="app-main">
        <section className="panel">
          {error && <p className="error">{error}</p>}
          {(["sources", "processors", "analyzers"] as const).map((groupKey) => {
            const typeLabel =
              groupKey === "sources"
                ? "Sources"
                : groupKey === "processors"
                  ? "Processors"
                  : "Analyzers";
            const typeConst =
              groupKey === "sources"
                ? "SOURCE"
                : groupKey === "processors"
                  ? "PROCESSOR"
                  : "ANALYZER";
            const list = grouped[groupKey];
            const availablePlugins = filteredPlugins(
              typeConst as "SOURCE" | "PROCESSOR" | "ANALYZER",
            );
            const runAllEnabled =
              groupKey === "sources" || groupKey === "processors" || groupKey === "analyzers";
            const hasProviders = list.length > 0;

            return (
              <div key={groupKey} className="subsection">
                <div className="panel-header">
                  <h3>{typeLabel}</h3>
                  <div className="panel-actions">
                    {runAllEnabled && (
                      <button
                        type="button"
                        className="app-btn btn-primary"
                        onClick={async () => {
                          setError(null);
                          try {
                            if (groupKey === "sources") {
                              await triggerScan(undefined);
                            } else if (groupKey === "processors") {
                              setScanningId(0);
                              const snap = await runAllProcessors();
                              navigate(`/snapshots/${snap.id}`);
                            } else if (groupKey === "analyzers") {
                              setScanningId(0);
                              await runAllAnalyzers();
                            }
                          } catch (err) {
                            const message = err instanceof Error ? err.message : String(err);
                            setError(message);
                          } finally {
                            setScanningId(null);
                          }
                        }}
                        disabled={scanningId !== null || loading || !hasProviders}
                        title={
                          hasProviders
                            ? groupKey === "sources"
                              ? "Scan all sources"
                              : groupKey === "processors"
                                ? "Run all processors on assets"
                                : "Run all analyzers"
                            : "Add a provider to enable this action"
                        }
                      >
                        {groupKey === "sources"
                          ? scanningId === null
                            ? "Scan all sources"
                            : "Starting..."
                          : groupKey === "processors"
                            ? "Run all processors"
                            : "Run all analyzers"}
                      </button>
                    )}
                    <button
                      type="button"
                      className="app-btn btn-primary"
                      onClick={() =>
                        navigate(
                          `/providers/new?type=${
                            groupKey === "sources"
                              ? "sources"
                              : groupKey === "processors"
                                ? "processors"
                                : "analyzers"
                          }`,
                        )
                      }
                      disabled={availablePlugins.length === 0}
                      title={
                        availablePlugins.length === 0
                          ? "No plugins installed for this type"
                          : undefined
                      }
                    >
                      Add
                    </button>
                  </div>
                </div>
                <div className="record-list">
                  {list.map((provider) => (
                    <div key={provider.id} className="file-card">
                      <div className="status-bar">
                        <strong>
                          #{provider.id} {provider.name}
                        </strong>
                        <span>{typeLabel}</span>
                      </div>
                      <p>Plugin: {provider.plugin_id ?? "n/a"}</p>
                      <div className="meta-grid">
                        <div>Created: {provider.created_at ?? "—"}</div>
                        <div>Updated: {provider.updated_at ?? "—"}</div>
                      </div>
                      <div className="button-row">
                        <Link to={`/providers/${provider.id}`} className="link-button">
                          Details
                        </Link>
                        <button
                          type="button"
                          className="app-btn btn-primary"
                          onClick={() => triggerScan(provider.id)}
                          disabled={scanningId !== null}
                        >
                          {scanningId === provider.id ? "Starting..." : "Scan"}
                        </button>
                      </div>
                    </div>
                  ))}
                  {!loading && list.length === 0 && (
                    <div className="empty-state">No {typeLabel.toLowerCase()} found.</div>
                  )}
                </div>
              </div>
            );
          })}
        </section>
      </main>
    </>
  );
}

export default ProvidersRoute;
