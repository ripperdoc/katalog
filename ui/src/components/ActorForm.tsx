import { useMemo } from "react";
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
  onSubmit: () => void;
  canSubmit: boolean;
  submitting: boolean;
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
  onSubmit,
  canSubmit,
  submitting,
  submitLabel = "Save",
  submittingLabel = "Saving...",
}: ActorFormProps) {
  const hasEditableSchema = useMemo(() => {
    if (!schema) return false;
    if (schema.type && schema.type !== "object") return true;
    const props = schema.properties;
    return !!props && typeof props === "object" && Object.keys(props).length > 0;
  }, [schema]);

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
                {plugin.title ?? plugin.plugin_id} ({plugin.type.toLowerCase()})
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
        <label className="form-row">
          <span>Config</span>
          <Form
            schema={schema as any}
            formData={configData}
            onChange={(evt) => onConfigChange(evt.formData as Record<string, unknown>)}
            liveValidate={false}
            validator={validator}
          >
            <div />
          </Form>
        </label>
      )}
      <div className="button-row form-actions">
        <button
          type="button"
          className="app-btn btn-primary"
          disabled={!canSubmit || submitting}
          onClick={onSubmit}
        >
          {submitting ? submittingLabel : submitLabel}
        </button>
      </div>
    </>
  );
}

export default ActorForm;
