import { ReactNode } from "react";

type ListColumn<T> = {
  accessor: string;
  label: string;
  width?: string | number;
  render?: (item: T) => ReactNode;
};

interface ListTableProps<T> {
  items: T[];
  columns: ListColumn<T>[];
  onRowClick?: (item: T) => void;
  emptyMessage?: string;
  loading?: boolean;
}

function ListTable<T>({
  items,
  columns,
  onRowClick,
  emptyMessage = "No records found.",
  loading = false,
}: ListTableProps<T>) {
  if (!loading && items.length === 0) {
    return <div className="empty-state">{emptyMessage}</div>;
  }

  return (
    <div className="table-responsive">
      <table className="collections-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.accessor} style={col.width ? { width: col.width } : undefined}>
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.map((item, idx) => (
            <tr
              key={idx}
              onClick={onRowClick ? () => onRowClick(item) : undefined}
              style={onRowClick ? { cursor: "pointer" } : undefined}
            >
              {columns.map((col) => (
                <td key={col.accessor}>
                  {col.render
                    ? col.render(item)
                    : // @ts-expect-error generic index
                      (item as any)[col.accessor] ?? ""}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export type { ListColumn };
export default ListTable;
