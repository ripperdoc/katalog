import { Link } from "react-router-dom";
import type { CellRendererProps } from "simple-table-core";
import { useRegistry } from "../utils/registry";

function ProviderCell({ value }: CellRendererProps) {
  const { data } = useRegistry();
  const providerId = typeof value === "number" ? value : Number(value);
  const providerName = data?.providersById?.[providerId]?.name;

  if (!providerId || Number.isNaN(providerId)) {
    return <span>{providerName ?? String(value ?? "")}</span>;
  }

  return <Link to={`/providers/${providerId}`}>{providerName ?? String(providerId)}</Link>;
}

export default ProviderCell;
