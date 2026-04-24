import type { CellRendererProps } from "@simple-table/react";
import AppLink from "./AppLink";

function AssetCell({ value }: CellRendererProps) {
  const assetId = typeof value === "number" ? value : Number(value);

  if (!assetId || Number.isNaN(assetId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return <AppLink to={`/assets/${assetId}`}>{String(assetId)}</AppLink>;
}

export default AssetCell;
