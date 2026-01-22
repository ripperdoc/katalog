import { Link } from "react-router-dom";
import { useChangesetProgress } from "../contexts/ChangesetProgressContext";

const truncate = (text: string, max: number) => {
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
};

function ChangesetProgressBar() {
  const { active } = useChangesetProgress();
  const current = active[0];

  if (!current) {
    return null;
  }

  const total =
    current.queued !== null && current.running !== null && current.finished !== null
      ? current.queued + current.running + current.finished
      : null;

  const finishedCount = current.finished ?? 0;
  const percent =
    total !== null && total > 0
      ? Math.min(100, Math.max(0, Math.round((finishedCount / total) * 100)))
      : null;

  const label = truncate(current.message ?? `Changeset #${current.id}`, 28);

  return (
    <Link to={`/changesets/${current.id}`} className="changeset-progress">
      <div className="changeset-progress__label">
        <span className="icon">hourglass_bottom</span>
        <span>{label}</span>
      </div>
      <div className="changeset-progress__bar">
        <div
          className={`changeset-progress__fill ${percent === null ? "indeterminate" : ""}`}
          style={percent !== null ? { width: `${percent}%` } : undefined}
        />
      </div>
      <div className="changeset-progress__meta">
        {percent !== null
          ? `${percent}% (${current.finished}/${total})`
          : "working…"}
      </div>
    </Link>
  );
}

export default ChangesetProgressBar;
