import { Link } from "react-router-dom";
import type { Actor } from "../types/api";

type ActorListProps = {
  actors: Actor[];
  typeLabel: string;
  runningId?: number | null;
  loading?: boolean;
  showEdit?: boolean;
  showToggle?: boolean;
  showRun?: boolean;
  runDisabled?: boolean;
  runContextLabel?: string;
  emptyLabel?: string;
  onRun?: (actor: Actor) => void;
  onToggleDisabled?: (actor: Actor) => void;
};

const ActorList = ({
  actors,
  typeLabel,
  runningId,
  loading = false,
  showEdit = true,
  showToggle = true,
  showRun = true,
  runDisabled = false,
  runContextLabel,
  emptyLabel,
  onRun,
  onToggleDisabled,
}: ActorListProps) => {
  return (
    <div className="record-list">
      {actors.map((actor) => (
        <div key={actor.id} className="file-card">
          <div className="status-bar">
            <strong>
              #{actor.id} {actor.name}
            </strong>
            <span>
              {typeLabel}
              {actor.disabled ? " · Disabled" : ""}
            </span>
          </div>
          <p>Plugin: {actor.plugin_id ?? "n/a"}</p>
          <div className="meta-grid">
            <div>Created: {actor.created_at ?? "—"}</div>
            <div>Updated: {actor.updated_at ?? "—"}</div>
          </div>
          <div className="button-row">
            {showToggle && (
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={!actor.disabled}
                  onChange={() => onToggleDisabled?.(actor)}
                />
                <span>{actor.disabled ? "Disabled" : "Enabled"}</span>
              </label>
            )}
            {showEdit && (
              <Link to={`/actors/${actor.id}`} className="link-button">
                Edit
              </Link>
            )}
            {showRun && (
              <button
                type="button"
                className="app-btn btn-action"
                onClick={() => onRun?.(actor)}
                disabled={runDisabled || actor.disabled}
                title={
                  runContextLabel
                    ? `Run ${typeLabel.toLowerCase()} on ${runContextLabel}`
                    : undefined
                }
              >
                {runningId === actor.id ? "Starting..." : "Run"}
              </button>
            )}
          </div>
        </div>
      ))}
      {!loading && actors.length === 0 && (
        <div className="empty-state">
          {emptyLabel ?? `No ${typeLabel.toLowerCase()} found.`}
        </div>
      )}
    </div>
  );
};

export default ActorList;
