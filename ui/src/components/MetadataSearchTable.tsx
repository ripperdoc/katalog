import { useMemo, useState } from "react";
import { HeaderObject, SimpleTable, ValueFormatterProps } from "simple-table-core";
import type { MetadataSearchHit } from "../types/api";
import AssetCell from "./AssetCell";
import { ActorCellPure } from "./ActorCell";
import TableFooter from "./TableFooter";
import { useRegistry } from "../utils/registry";

type MetadataSearchTableProps = {
  items: MetadataSearchHit[];
  loading: boolean;
  limit: number;
  page: number;
  total: number | null;
  queryTimeMs?: number | null;
  onPageChange: (page: number) => void;
  onSelectionChange?: (selectedAssetIds: Set<number>) => void;
};

const formatMetadataValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

const formatScore = (props: ValueFormatterProps): string => {
  const rawValue = props.row[props.accessor];
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return "";
  }
  const numericValue = typeof rawValue === "number" ? rawValue : Number(rawValue);
  if (!Number.isFinite(numericValue)) {
    return "";
  }
  return numericValue.toFixed(3);
};

const formatDistance = (props: ValueFormatterProps): string => {
  const rawValue = props.row[props.accessor];
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return "";
  }
  const numericValue = typeof rawValue === "number" ? rawValue : Number(rawValue);
  if (!Number.isFinite(numericValue)) {
    return "";
  }
  return numericValue.toFixed(4);
};

function MetadataSearchTable({
  items,
  loading,
  limit,
  page,
  total,
  queryTimeMs,
  onPageChange,
  onSelectionChange,
}: MetadataSearchTableProps) {
  const { data: registryData } = useRegistry();
  const [selectedAssetIds, setSelectedAssetIds] = useState<Set<number>>(new Set());

  const headers = useMemo<HeaderObject[]>(
    () => [
      {
        accessor: "asset_id",
        label: "Asset",
        width: 110,
        type: "number",
        cellRenderer: AssetCell,
      },
      {
        accessor: "metadata_key",
        label: "Metadata key",
        width: "1.3fr",
        type: "string",
      },
      {
        accessor: "value_text",
        label: "Matched value",
        width: "3fr",
        type: "string",
      },
      {
        accessor: "score",
        label: "Semantic",
        width: 105,
        type: "number",
        valueFormatter: formatScore,
      },
      {
        accessor: "distance",
        label: "Distance",
        width: 105,
        type: "number",
        valueFormatter: formatDistance,
      },
      {
        accessor: "fts_rank",
        label: "FTS rank",
        width: 105,
        type: "number",
        valueFormatter: formatDistance,
      },
      {
        accessor: "actor_id",
        label: "Actor",
        width: 130,
        type: "number",
        cellRenderer: (props) => (
          <ActorCellPure {...props} actorsById={registryData?.actorsById} />
        ),
      },
    ],
    [registryData],
  );

  const rows = useMemo(
    () =>
      items.map((item, index) => ({
        id:
          item.metadata_id ??
          `${item.asset_id}:${item.metadata_key_id ?? item.metadata_key}:${index}`,
        asset_id: item.asset_id,
        metadata_key: item.metadata_key,
        value_text: formatMetadataValue(item.value ?? item.text),
        score: item.score ?? item.cosine_similarity ?? null,
        distance: item.distance ?? null,
        fts_rank: item.fts_rank ?? null,
        actor_id: item.actor_id ?? null,
      })),
    [items],
  );
  const assetIdByRowId = useMemo(() => {
    const lookup = new Map<string, number>();
    for (const row of rows) {
      lookup.set(String(row.id), Number(row.asset_id));
    }
    return lookup;
  }, [rows]);

  return (
    <SimpleTable
      defaultHeaders={headers}
      rows={rows}
      height="100%"
      autoExpandColumns={false}
      selectableCells={true}
      columnResizing={true}
      shouldPaginate={true}
      rowsPerPage={limit}
      serverSidePagination={true}
      enableRowSelection={true}
      totalRowCount={total ?? rows.length}
      footerRenderer={(props) => (
        <TableFooter
          {...props}
          queryTimeMs={queryTimeMs}
          selectedCount={selectedAssetIds.size}
        />
      )}
      onPageChange={(nextPage) => {
        if (nextPage === page) {
          return;
        }
        onPageChange(nextPage);
      }}
      isLoading={loading}
      onRowSelectionChange={({ selectedRows }) => {
        const nextSelected = new Set<number>();
        for (const selectedRowId of selectedRows) {
          const assetId = assetIdByRowId.get(String(selectedRowId));
          if (assetId !== undefined && !Number.isNaN(assetId)) {
            nextSelected.add(assetId);
          }
        }
        setSelectedAssetIds(nextSelected);
        onSelectionChange?.(nextSelected);
      }}
    />
  );
}

export default MetadataSearchTable;
