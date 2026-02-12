import type { WorkflowSummary } from "../types/api";

type WorkflowListProps = {
  workflows: WorkflowSummary[];
  loading?: boolean;
  runningName?: string | null;
  onSync?: (workflow: WorkflowSummary) => void;
  onRun?: (workflow: WorkflowSummary) => void;
  onApply?: (workflow: WorkflowSummary) => void;
};

const WorkflowList = ({
  workflows,
  loading = false,
  runningName = null,
  onSync,
  onRun,
  onApply,
}: WorkflowListProps) => {
  return (
    <div className="record-list">
      {workflows.map((workflow) => (
        <div key={workflow.file_name} className="file-card">
          <div className="status-bar">
            <strong>{workflow.name}</strong>
            <span>
              Workflow
              {workflow.version ? ` · v${workflow.version}` : ""}
            </span>
          </div>
          <p>{workflow.description || workflow.file_name}</p>
          <div className="meta-grid">
            <div>File: {workflow.file_name}</div>
            <div>Status: {workflow.status}</div>
            <div>Actors: {workflow.actor_count}</div>
            <div>Sources: {workflow.source_count}</div>
            <div>Processors: {workflow.processor_count}</div>
          </div>
          <div className="meta-grid">
            <div>
              <strong>All actors:</strong> {workflow.actor_names.join(", ") || "—"}
            </div>
            {workflow.processor_stages.map((stage, index) => (
              <div key={`${workflow.file_name}-stage-${index}`}>
                <strong>Stage {index + 1}:</strong> {stage.join(", ") || "—"}
              </div>
            ))}
          </div>
          {workflow.error && <p className="error">{workflow.error}</p>}
          <div className="button-row">
            <button
              type="button"
              className="app-btn btn-save"
              onClick={() => onSync?.(workflow)}
              disabled={runningName !== null}
            >
              {runningName === workflow.file_name ? "Working..." : "Sync"}
            </button>
            <button
              type="button"
              className="app-btn btn-action"
              onClick={() => onRun?.(workflow)}
              disabled={runningName !== null || workflow.status !== "ready"}
            >
              {runningName === workflow.file_name ? "Working..." : "Run"}
            </button>
            <button
              type="button"
              className="app-btn btn-action"
              onClick={() => onApply?.(workflow)}
              disabled={runningName !== null}
            >
              {runningName === workflow.file_name ? "Working..." : "Apply"}
            </button>
          </div>
        </div>
      ))}
      {!loading && workflows.length === 0 && (
        <div className="empty-state">No workflows found.</div>
      )}
    </div>
  );
};

export default WorkflowList;
