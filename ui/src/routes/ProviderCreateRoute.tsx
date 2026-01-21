import { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import {
  createProvider,
  fetchPlugins,
  fetchPluginConfigSchema,
  type PluginSpec,
} from "../api/client";

const USER_EDITOR_PLUGIN_ID = "katalog.sources.user_editor.UserEditorSource";

function ProviderCreateRoute() {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const [plugins, setPlugins] = useState<PluginSpec[]>([]);
  const [pluginId, setPluginId] = useState<string>("");
  const [name, setName] = useState<string>("");
  const [schema, setSchema] = useState<Record<string, unknown>>({ type: "object" });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const typeFilter = useMemo(() => {
    const t = params.get("type");
    if (!t) return null;
    const upper = t.toUpperCase();
    return upper === "SOURCES" ? "SOURCE" : upper === "PROCESSORS" ? "PROCESSOR" : upper === "ANALYZERS" ? "ANALYZER" : null;
  }, [params]);

  const loadPlugins = useCallback(async () => {
    setError(null);
    try {
      const res = await fetchPlugins();
      const list = res.plugins ?? [];
      const filtered = typeFilter ? list.filter((p) => p.type === typeFilter) : list;
      setPlugins(filtered);
      const defaultPlugin = filtered.find((p) => p.plugin_id === USER_EDITOR_PLUGIN_ID) || filtered[0];
      if (defaultPlugin) {
        setPluginId(defaultPlugin.plugin_id);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
      setPlugins([]);
    }
  }, [typeFilter]);

  useEffect(() => {
    void loadPlugins();
  }, [loadPlugins]);

  useEffect(() => {
    const loadSchema = async () => {
      if (!pluginId) {
        setSchema({ type: "object" });
        return;
      }
      try {
        const res = await fetchPluginConfigSchema(pluginId);
        setSchema(res.schema || { type: "object" });
      } catch {
        setSchema({ type: "object" });
      }
    };
    void loadSchema();
  }, [pluginId]);

  const handleSubmit = async ({ formData }: { formData: Record<string, unknown> }) => {
    if (!pluginId) {
      setError("Select a plugin");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await createProvider({
        name: name || pluginId,
        plugin_id: pluginId,
        config: formData,
      });
      navigate(`/providers/${res.provider.id}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="panel">
      <header className="panel-header">
        <div>
          <h2>Create provider</h2>
          <p>Select a plugin and configure it.</p>
        </div>
        <div className="button-row">
          <Link to="/providers" className="link-button">
            Back
          </Link>
        </div>
      </header>
      {error && <p className="error">{error}</p>}
      <div className="file-card">
        <label className="form-row">
          <span>Plugin</span>
          <select value={pluginId} onChange={(e) => setPluginId(e.target.value)}>
            {plugins.map((p) => (
              <option key={p.plugin_id} value={p.plugin_id}>
                {p.title ?? p.plugin_id} ({p.type.toLowerCase()})
              </option>
            ))}
          </select>
        </label>
        <label className="form-row">
          <span>Name</span>
          <input
            type="text"
            placeholder="Friendly name"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </label>
        <Form
          schema={schema as any}
          formData={{}}
          onSubmit={(evt) => void handleSubmit(evt)}
          validator={validator}
        >
          <div className="button-row">
            <button type="submit" disabled={loading || !pluginId}>
              {loading ? "Saving..." : "Create"}
            </button>
          </div>
        </Form>
      </div>
    </section>
  );
}

export default ProviderCreateRoute;
