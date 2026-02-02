import { Link } from "react-router-dom";
import type { CellRendererProps } from "simple-table-core";

function AssetCell({ value }: CellRendererProps) {
  const assetId = typeof value === "number" ? value : Number(value);

  if (!assetId || Number.isNaN(assetId)) {
    return <span>{String(value ?? "")}</span>;
  }

  return <Link to={`/assets/${assetId}`}>{String(assetId)}</Link>;
}

export default AssetCell;
