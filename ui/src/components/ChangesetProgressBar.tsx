import { KeyboardEvent, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  cancelChangeset,
  deleteChangeset,
  finishChangeset,
  updateChangesetMessage,
} from "../api/client";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

const truncate = (text: string, max: number) => {
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
};

function ChangesetProgressBar() {
  const { active, stopTracking } = useChangesetProgress();
  const current = active[0];
  const [messageDraft, setMessageDraft] = useState("");
  const [savingMessage, setSavingMessage] = useState(false);
  const [finishing, setFinishing] = useState(false);
  const [cancelling, setCancelling] = useState(false);

  const isManual = Boolean(current && current.data && current.data["manual"]);

  const total =
    current &&
    current.queued !== null &&
    current.running !== null &&
    current.finished !== null
      ? current.queued + current.running + current.finished
      : null;
  const hasUnknownTotal =
    current &&
    current.queued === null &&
    current.running !== null &&
    current.finished !== null;

  const finishedCount = current?.finished ?? 0;
  const percent =
    total !== null && total > 0
      ? Math.min(100, Math.max(0, Math.round((finishedCount / total) * 100)))
      : hasUnknownTotal
        ? 50
        : null;

  const kind = current?.kind ?? "tasks";
  const displayMessage =
    isManual && messageDraft.trim().length > 0
      ? messageDraft.trim()
      : total === null && current?.logMessage
        ? current.logMessage
        : current?.message;
  const label = truncate(
    displayMessage ?? (current ? `Changeset #${current.id}` : "Changeset"),
    28,
  );
  const progressLabel =
    total !== null
      ? `${percent ?? 0}% (${current.finished}/${total} ${kind})`
      : hasUnknownTotal
        ? `50% (${current.finished}/unknown ${kind})`
        : isManual
          ? "manual edit"
          : "working…";

  useEffect(() => {
    if (!current) {
      return;
    }
    setMessageDraft(current.message ?? "");
  }, [current?.id, current?.message]);

  if (!current) {
    return null;
  }

  const handleMessageSave = async () => {
    if (!isManual || savingMessage) {
      return;
    }
    setSavingMessage(true);
    try {
      await updateChangesetMessage(current.id, messageDraft.trim());
    } catch {
      // ignore update failure; message can be retried
    } finally {
      setSavingMessage(false);
    }
  };

  const handleMessageKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void handleMessageSave();
    }
  };

  const handleFinish = async () => {
    if (finishing) {
      return;
    }
    setFinishing(true);
    try {
      await finishChangeset(current.id);
      stopTracking(current.id);
    } catch {
      // ignore failures
    } finally {
      setFinishing(false);
    }
  };

  const handleCancel = async () => {
    if (cancelling) {
      return;
    }
    setCancelling(true);
    try {
      if (isManual) {
        await deleteChangeset(current.id);
      } else {
        await cancelChangeset(current.id);
      }
      stopTracking(current.id);
    } catch {
      // ignore failures
    } finally {
      setCancelling(false);
    }
  };

  return (
    <div className="changeset-dock" role="status" aria-live="polite">
      <div className="changeset-dock__body">
        <Link to={`/changesets/${current.id}`} className="changeset-dock__link">
          <div className="changeset-dock__label">
            <span className="icon">hourglass_bottom</span>
            <span>{label}</span>
          </div>
          <div className="changeset-dock__bar">
            <div
              className={`changeset-dock__fill ${percent === null ? "indeterminate" : ""}`}
              style={percent !== null ? { width: `${percent}%` } : undefined}
            />
          </div>
          <div className="changeset-dock__meta">{progressLabel}</div>
        </Link>
        <div className="changeset-dock__actions">
          {isManual && (
            <button
              type="button"
              className="app-btn btn-save"
              onClick={() => void handleFinish()}
              disabled={finishing || cancelling}
            >
              {finishing ? "Finishing…" : "Finish"}
            </button>
          )}
          <button
            type="button"
            className="app-btn danger"
            onClick={() => void handleCancel()}
            disabled={cancelling || finishing}
          >
            {cancelling ? "Cancelling…" : "Cancel"}
          </button>
        </div>
      </div>
      {isManual && (
        <div className="changeset-dock__message">
          <label htmlFor="changeset-message">Message</label>
          <input
            id="changeset-message"
            type="text"
            value={messageDraft}
            onChange={(event) => setMessageDraft(event.target.value)}
            onBlur={() => void handleMessageSave()}
            onKeyDown={handleMessageKeyDown}
            placeholder="Describe this changeset…"
            disabled={savingMessage}
          />
        </div>
      )}
    </div>
  );
}

export default ChangesetProgressBar;
