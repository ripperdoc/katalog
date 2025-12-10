import { useMemo } from "react";
import { SimpleTable, HeaderObject, Row } from "simple-table-core";

const buildTestData = (count: number): Row[] =>
  Array.from({ length: count }, (_, index) => ({
    id: `file${index + 1}`,
    provider_id: "sourceA",
  }));

function Test() {
  const rows = useMemo(() => buildTestData(60), []);
  const headers: HeaderObject[] = [
    { accessor: "id", label: "ID", width: "1fr", type: "string" },
    { accessor: "provider_id", label: "Source", width: "1fr", type: "string" },
  ];

  return (
    <section className="panel">
      <SimpleTable rowIdAccessor="id" rows={rows} defaultHeaders={headers} height={"100%"} />
    </section>
  );
}

export default Test;
