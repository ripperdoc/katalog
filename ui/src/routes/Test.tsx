import { useMemo } from "react";
import { SimpleTable, HeaderObject, Row } from "simple-table-core";

const buildTestData = (count: number): Row[] =>
  Array.from({ length: count }, (_, index) => ({
    id: index + 1,
    canonical_id: `canonical-${index + 1}`,
    canonical_uri: `katalog://source/asset/${index + 1}`,
    created: 1000 + index,
    seen: 2000 + index,
    deleted: null,
    metadata: {
      "file/name": { value: `file-${index + 1}.txt`, count: 1 },
    },
  }));

function Test() {
  const rows = useMemo(() => buildTestData(60), []);
  const headers: HeaderObject[] = [
    { accessor: "id", label: "ID", width: "1fr", type: "number" },
    { accessor: "canonical_id", label: "Canonical ID", width: "1fr", type: "string" },
    { accessor: "canonical_uri", label: "URI", width: "2fr", type: "string" },
    { accessor: "seen", label: "Last Snapshot", width: "1fr", type: "number" },
  ];

  return (
    <section className="panel">
      <SimpleTable rowIdAccessor="id" rows={rows} defaultHeaders={headers} height={"100%"} />
    </section>
  );
}

export default Test;
