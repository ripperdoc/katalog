import type { CellRendererProps } from "simple-table-core";
import type { MetadataValueEntry } from "../types/api";

const extractValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    const entry = value as MetadataValueEntry;
    const inner = entry.value;
    if (inner === null || inner === undefined) {
      return "";
    }
    if (typeof inner === "object") {
      return JSON.stringify(inner);
    }
    return String(inner);
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
};

function ExternalIdCell({ value, row, formattedValue }: CellRendererProps) {
  const displayValue =
    formattedValue === undefined ? extractValue(value) : String(formattedValue ?? "");
  const canonicalUri = extractValue(row["asset/canonical_uri"]);

  if (!canonicalUri) {
    return <span>{displayValue}</span>;
  }

  const linkLabel = displayValue || canonicalUri;

  return (
    <a href={canonicalUri} target="_blank" rel="noopener noreferrer">
      {linkLabel}
    </a>
  );
}

export default ExternalIdCell;
