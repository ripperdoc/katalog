import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import {
  fetchPlugins,
  fetchProviders,
  createProvider,
  updateProvider,
  fetchProviderConfigSchema,
  fetchPluginConfigSchema,
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
  const [formMode, setFormMode] = useState<"create" | "edit" | null>(null);
  const [formType, setFormType] = useState<"source" | "processor" | "analyzer">("source");
  const [editingId, setEditingId] = useState<number | null>(null);
  const [formName, setFormName] = useState("");
  const [formPluginId, setFormPluginId] = useState("");
  const [configSchema, setConfigSchema] = useState<Record<string, unknown>>({ type: "object" });
  const [configData, setConfigData] = useState<Record<string, unknown>>({});
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
      const defaults = response.plugins ?? [];
      if (defaults.length > 0) {
        const firstSource = defaults.find((p) => p.type === "SOURCE");
        setFormPluginId(firstSource?.plugin_id || defaults[0].plugin_id);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
      setPlugins([]);
    }
  }, []);

  const normalizeSchema = (schema: Record<string, unknown>): Record<string, unknown> => {
    const clone = structuredClone(schema) as any;
    const walk = (node: any) => {
      if (node && typeof node === "object") {
        if (Array.isArray(node.anyOf) && node.anyOf.length === 2) {
          const hasString = node.anyOf.find((n: any) => n.type === "string");
          const hasNull = node.anyOf.find((n: any) => n.type === "null");
          if (hasString && hasNull) {
            node.type = ["string", "null"];
            if (hasString.format) {
              node.format = hasString.format;
            }
            delete node.anyOf;
          }
        }
        if (node.properties && typeof node.properties === "object") {
          Object.values(node.properties).forEach(walk);
        }
        if (node.items) {
          walk(node.items);
        }
      }
    };
    walk(clone);
    return clone;
  };


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

  const handleSave = async () => {
    setError(null);
    try {
      if (formMode === "edit" && editingId) {
        await updateProvider(editingId, {
          name: formName || undefined,
          config: configData,
        });
      } else {
        await createProvider({
          name: formName || formPluginId,
          plugin_id: formPluginId,
          config: configData,
        });
      }
      resetForm();
      await loadProviders();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setError(message);
    }
  };

  const resetForm = () => {
    setFormMode(null);
    setFormType("source");
    setEditingId(null);
    setFormName("");
    setFormPluginId("");
    setConfigSchema({ type: "object" });
    setConfigData({});
  };

  const grouped = {
    sources: providers.filter((p) => p.type === "SOURCE"),
    processors: providers.filter((p) => p.type === "PROCESSOR"),
    analyzers: providers.filter((p) => p.type === "ANALYZER"),
  };

  const filteredPlugins = (ptype: "SOURCE" | "PROCESSOR" | "ANALYZER") =>
    plugins.filter((p) => p.type === ptype);

  const openCreate = async (ptype: "source" | "processor" | "analyzer") => {
    const typeKey = ptype.toUpperCase() as "SOURCE" | "PROCESSOR" | "ANALYZER";
    const available = filteredPlugins(typeKey);
    setFormType(ptype);
    setFormMode("create");
    setEditingId(null);
    setFormName("");
    const nextPlugin = available[0]?.plugin_id ?? "";
    setFormPluginId(nextPlugin);
    setConfigData({});
    if (nextPlugin) {
      try {
        const schemaRes = await fetchPluginConfigSchema(nextPlugin);
        setConfigSchema(normalizeSchema(schemaRes.schema || { type: "object" }));
      } catch {
        setConfigSchema({ type: "object" });
      }
    } else {
      setConfigSchema({ type: "object" });
    }
  };

  const openEdit = async (provider: Provider) => {
    setEditingId(provider.id);
    setFormMode("edit");
    setFormName(provider.name);
    setFormPluginId(provider.plugin_id || "");
    const kind =
      provider.type === "PROCESSOR"
        ? "processor"
        : provider.type === "ANALYZER"
          ? "analyzer"
          : "source";
    setFormType(kind);
    try {
      const schemaRes = await fetchProviderConfigSchema(provider.id);
      setConfigSchema(normalizeSchema(schemaRes.schema || { type: "object" }));
      setConfigData(schemaRes.value || {});
    } catch {
      setConfigSchema({ type: "object" });
      setConfigData(provider.config ?? {});
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Providers</h2>
          <p>Configured source, processor, and analyzer providers.</p>
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      {(["sources", "processors", "analyzers"] as const).map((groupKey) => {
        const typeLabel =
          groupKey === "sources" ? "Sources" : groupKey === "processors" ? "Processors" : "Analyzers";
        const typeConst =
          groupKey === "sources" ? "SOURCE" : groupKey === "processors" ? "PROCESSOR" : "ANALYZER";
        const list = grouped[groupKey];
        const availablePlugins = filteredPlugins(typeConst as "SOURCE" | "PROCESSOR" | "ANALYZER");
        const runAllEnabled = groupKey === "sources" || groupKey === "processors" || groupKey === "analyzers";
        const hasProviders = list.length > 0;

        const renderProviderCard = (
          provider: Provider | null,
          mode: "view" | "edit" | "create"
        ) => {
          const isCreate = mode === "create";
          const isEdit = mode === "edit";
          const pluginOptions = availablePlugins.map((plugin) => (
            <option key={plugin.plugin_id} value={plugin.plugin_id}>
              {plugin.title ?? plugin.plugin_id} ({plugin.type.toLowerCase()})
            </option>
          ));
          return (
            <div
              className="file-card"
              key={
                isCreate ? "create-form" : isEdit ? `edit-form-${provider?.id}` : `view-${provider?.id}`
              }
            >
              <div className="status-bar">
                <strong>
                  {isCreate ? "New provider" : `#${provider?.id ?? ""} ${provider?.name ?? ""}`}
                </strong>
                <span>{typeLabel}</span>
              </div>
              <p>Plugin: {provider?.plugin_id ?? formPluginId ?? "n/a"}</p>
              <div className="meta-grid">
                <div>Created: {provider?.created_at ?? "—"}</div>
                <div>Updated: {provider?.updated_at ?? "—"}</div>
              </div>
              <label className="form-row">
                <span>Plugin</span>
                <select
                  value={isCreate ? formPluginId : provider?.plugin_id || formPluginId}
                  onChange={async (e) => {
                    const next = e.target.value;
                    setFormPluginId(next);
                    setConfigData({});
                    try {
                      const schemaRes = await fetchPluginConfigSchema(next);
                      setConfigSchema(schemaRes.schema || { type: "object" });
                    } catch {
                      setConfigSchema({ type: "object" });
                    }
                  }}
                  disabled={isEdit}
                >
                  {pluginOptions}
                </select>
              </label>
              <label className="form-row">
                <span>Name</span>
                <input
                  type="text"
                  placeholder="Friendly name"
                  value={isCreate || isEdit ? formName : provider?.name || ""}
                  onChange={(e) => setFormName(e.target.value)}
                  disabled={mode === "view"}
                />
              </label>
              <label className="form-row">
                <span>Config</span>
                <Form
                  schema={configSchema as any}
                  formData={mode === "view" ? provider?.config ?? {} : configData}
                  onChange={(evt) => setConfigData(evt.formData as Record<string, unknown>)}
                  liveValidate={false}
                  disabled={mode === "view"}
                  validator={validator}
                >
                  <div />
                </Form>
              </label>
              <div className="button-row">
                {mode === "view" ? (
                  <>
                    <Link to={`/providers/${provider?.id}`} className="link-button">
                      Details
                    </Link>
                    <button type="button" onClick={() => void openEdit(provider!)}>
                      Edit
                    </button>
                    <button
                      type="button"
                      onClick={() => triggerScan(provider?.id)}
                      disabled={scanningId !== null}
                    >
                      {scanningId === provider?.id ? "Starting..." : "Scan"}
                    </button>
                  </>
                ) : (
                  <>
                    <button type="button" onClick={handleSave} disabled={!formPluginId || loading}>
                      {isEdit ? "Save" : "Create"}
                    </button>
                    <button type="button" onClick={resetForm}>
                      Cancel
                    </button>
                  </>
                )}
              </div>
            </div>
          );
        };

        return (
          <div key={groupKey} className="subsection">
            <div className="panel-header">
              <h3>{typeLabel}</h3>
              <div className="panel-actions">
                {runAllEnabled && (
                  <button
                    type="button"
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
                  onClick={() =>
                    openCreate(
                      groupKey === "sources" ? "source" : groupKey === "processors" ? "processor" : "analyzer"
                    )
                  }
                  disabled={availablePlugins.length === 0}
                  title={availablePlugins.length === 0 ? "No plugins installed for this type" : undefined}
                >
                  Add
                </button>
              </div>
            </div>
            <div className="record-list">
              {formMode === "create" &&
                formType ===
                  (groupKey === "sources"
                    ? "source"
                    : groupKey === "processors"
                      ? "processor"
                      : "analyzer") &&
                renderProviderCard(null, "create")}
              {list.map((provider) =>
                formMode === "edit" && editingId === provider.id
                  ? renderProviderCard(provider, "edit")
                  : renderProviderCard(provider, "view")
              )}
              {!loading && list.length === 0 && <div className="empty-state">No {typeLabel.toLowerCase()} found.</div>}
            </div>
          </div>
        );
      })}
    </section>
  );
}

export default ProvidersRoute;
