import { useMemo, useState } from "react";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import type { PluginSpec } from "../types/api";

type ActorFormProps = {
  isCreating: boolean;
  plugins?: PluginSpec[];
  pluginId: string;
  onPluginChange?: (pluginId: string) => void;
  name: string;
  onNameChange: (value: string) => void;
  schema: Record<string, unknown> | null;
  configData: Record<string, unknown>;
  onConfigChange: (value: Record<string, unknown>) => void;
  configToml: string;
  onConfigTomlChange: (value: string) => void;
  onSubmit: () => void;
  canSubmit: boolean;
  submitting: boolean;
  showSubmit?: boolean;
  submitLabel?: string;
  submittingLabel?: string;
};

function ActorForm({
  isCreating,
  plugins = [],
  pluginId,
  onPluginChange,
  name,
  onNameChange,
  schema,
  configData,
  onConfigChange,
  configToml,
  onConfigTomlChange,
  onSubmit,
  canSubmit,
  submitting,
  showSubmit = true,
  submitLabel = "Save",
  submittingLabel = "Saving...",
}: ActorFormProps) {
  const [activeTab, setActiveTab] = useState<"form" | "toml">("form");

  const hasEditableSchema = useMemo(() => {
    if (!schema) return false;
    if (schema.type && schema.type !== "object") return true;
    const props = schema.properties;
    return !!props && typeof props === "object" && Object.keys(props).length > 0;
  }, [schema]);

  const usingToml = configToml.trim().length > 0;
  const formDisabled = usingToml;

  return (
    <>
      <label className="form-row">
        <span>Plugin</span>
        {isCreating ? (
          <select
            value={pluginId}
            onChange={(e) => onPluginChange?.(e.target.value)}
            disabled={plugins.length === 0}
          >
            {plugins.map((plugin) => (
              <option key={plugin.plugin_id} value={plugin.plugin_id}>
                {plugin.title ?? plugin.plugin_id} ({plugin.actor_type.toLowerCase()})
              </option>
            ))}
          </select>
        ) : (
          <input type="text" value={pluginId} disabled />
        )}
      </label>
      <label className="form-row">
        <span>Name</span>
        <input
          type="text"
          placeholder="Friendly name"
          value={name}
          onChange={(e) => onNameChange(e.target.value)}
        />
      </label>
      {hasEditableSchema && schema && (
        <div className="form-row">
          <span>Config</span>
          <div className="tabs">
            <ul className="tab-list" role="tablist">
              <li role="presentation">
                <button
                  type="button"
                  role="tab"
                  aria-selected={activeTab === "form"}
                  aria-controls="form-panel"
                  className={`tab-button ${activeTab === "form" ? "active" : ""}`}
                  onClick={() => setActiveTab("form")}
                  disabled={formDisabled}
                  title={formDisabled ? "Form disabled while using TOML config" : ""}
                >
                  Form
                </button>
              </li>
              <li role="presentation">
                <button
                  type="button"
                  role="tab"
                  aria-selected={activeTab === "toml"}
                  aria-controls="toml-panel"
                  className={`tab-button ${activeTab === "toml" ? "active" : ""}`}
                  onClick={() => setActiveTab("toml")}
                >
                  TOML
                </button>
              </li>
            </ul>

            {/* Form Tab */}
            <div
              id="form-panel"
              role="tabpanel"
              className={`tab-panel ${activeTab === "form" ? "" : "hidden"}`}
            >
              {formDisabled && (
                <div className="warning-message">
                  Form is disabled because TOML configuration is active. Switch to the TOML tab and
                  clear it to use the form editor.
                </div>
              )}
              {!formDisabled && (
                <Form
                  className="config-form"
                  schema={schema as any}
                  formData={configData}
                  onChange={(evt) => onConfigChange(evt.formData as Record<string, unknown>)}
                  liveValidate={false}
                  validator={validator}
                  uiSchema={{ "ui:submitButtonOptions": { norender: true } }}
                >
                  <></>
                </Form>
              )}
            </div>

            {/* TOML Tab */}
            <div
              id="toml-panel"
              role="tabpanel"
              className={`tab-panel ${activeTab === "toml" ? "" : "hidden"}`}
            >
              <textarea
                className="toml-textarea"
                value={configToml}
                onChange={(e) => onConfigTomlChange(e.target.value)}
                placeholder="# Enter TOML configuration here"
              />
              <div className="toml-actions">
                {usingToml && (
                  <button
                    type="button"
                    className="app-btn"
                    onClick={() => {
                      if (
                        window.confirm("Clear TOML configuration and switch back to form mode?")
                      ) {
                        onConfigTomlChange("");
                        setActiveTab("form");
                      }
                    }}
                  >
                    Clear TOML
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
      {showSubmit && (
        <div className="button-row form-actions">
          <button
            type="button"
            className="app-btn btn-save"
            disabled={!canSubmit || submitting}
            onClick={onSubmit}
          >
            {submitting ? submittingLabel : submitLabel}
          </button>
        </div>
      )}
    </>
  );
}

export default ActorForm;
