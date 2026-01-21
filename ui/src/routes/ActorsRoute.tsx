import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import AppHeader from "../components/AppHeader";
import {
  fetchPlugins,
  fetchActors,
  startScan,
  runAllProcessors,
  runAllAnalyzers,
} from "../api/client";
import type { Actor, PluginSpec } from "../types/api";

function ActorsRoute() {
  const [actors, setActors] = useState<Actor[]>([]);
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scanningId, setScanningId] = useState<number | null>(null);
  const navigate = useNavigate();
  const didInit = useRef(false);

  const loadActors = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchActors();
      setActors(response.actors ?? []);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setActors([]);
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
    void loadActors();
    void loadPlugins();
  }, [loadActors, loadPlugins]);

  const triggerScan = async (actorId?: number) => {
    setScanningId(actorId ?? 0);
    setError(null);
    try {
      const changeset = await startScan(actorId);
      navigate(`/changesets/${changeset.id}`);
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
    sources: actors.filter((p) => p.type === "SOURCE"),
    processors: actors.filter((p) => p.type === "PROCESSOR"),
    analyzers: actors.filter((p) => p.type === "ANALYZER"),
  };

  return (
    <>
      <AppHeader>
        <div>
          <h2>Actors</h2>
          <p>Configured source, processor, and analyzer actors.</p>
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
            const hasActors = list.length > 0;

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
                              navigate(`/changesets/${snap.id}`);
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
                        disabled={scanningId !== null || loading || !hasActors}
                        title={
                          hasActors
                            ? groupKey === "sources"
                              ? "Scan all sources"
                              : groupKey === "processors"
                                ? "Run all processors on assets"
                                : "Run all analyzers"
                            : "Add an actor to enable this action"
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
                          `/actors/new?type=${
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
                  {list.map((actor) => (
                    <div key={actor.id} className="file-card">
                      <div className="status-bar">
                        <strong>
                          #{actor.id} {actor.name}
                        </strong>
                        <span>{typeLabel}</span>
                      </div>
                      <p>Plugin: {actor.plugin_id ?? "n/a"}</p>
                      <div className="meta-grid">
                        <div>Created: {actor.created_at ?? "—"}</div>
                        <div>Updated: {actor.updated_at ?? "—"}</div>
                      </div>
                      <div className="button-row">
                        <Link to={`/actors/${actor.id}`} className="link-button">
                          Details
                        </Link>
                        <button
                          type="button"
                          className="app-btn btn-primary"
                          onClick={() => triggerScan(actor.id)}
                          disabled={scanningId !== null}
                        >
                          {scanningId === actor.id ? "Starting..." : "Scan"}
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

export default ActorsRoute;
