import type { CellRendererProps } from "simple-table-core";
import type { MetadataValueEntry } from "../types/api";

type FlagCellConfig = {
  label: string;
  iconOn: string;
  iconOff?: string;
  onColor?: string;
  offColor?: string;
};

const extractValue = (value: unknown): unknown => {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    return (value as MetadataValueEntry).value;
  }
  return value;
};

const isTruthy = (value: unknown): boolean => {
  if (value === null || value === undefined) {
    return false;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "" || normalized === "0" || normalized === "false") {
      return false;
    }
    if (normalized === "1" || normalized === "true" || normalized === "yes") {
      return true;
    }
    return true;
  }
  return Boolean(value);
};

export const makeFlagCell = (config: FlagCellConfig) => {
  const FlagCell = ({ value }: CellRendererProps) => {
    const resolved = extractValue(value);
    const active = isTruthy(resolved);
    const icon = active ? config.iconOn : config.iconOff ?? config.iconOn;
    const color = active ? config.onColor : config.offColor;

    return (
      <span
        className={`asset-flag-icon ${active ? "is-on" : "is-off"}`}
        title={config.label}
        style={color ? { color } : undefined}
        aria-label={config.label}
      >
        <span className="icon" aria-hidden="true">
          {icon}
        </span>
      </span>
    );
  };

  return FlagCell;
};

export const ThumbnailCell = ({ value }: CellRendererProps) => {
  const resolved = extractValue(value);
  const url = typeof resolved === "string" ? resolved.trim() : "";
  const hasThumbnail = url.length > 0;

  return (
    <span className={`asset-thumbnail-cell ${hasThumbnail ? "has-thumb" : "no-thumb"}`}>
      <span
        className={`asset-flag-icon ${hasThumbnail ? "is-on" : "is-off"}`}
        title={hasThumbnail ? "Thumbnail" : "No thumbnail"}
        aria-label={hasThumbnail ? "Thumbnail available" : "No thumbnail"}
      >
        <span className="icon" aria-hidden="true">
          image
        </span>
      </span>
      {hasThumbnail && (
        <span className="asset-thumbnail-preview" role="tooltip">
          <img src={url} alt="Thumbnail preview" loading="lazy" />
        </span>
      )}
    </span>
  );
};

export default makeFlagCell;
