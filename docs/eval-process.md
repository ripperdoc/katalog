# Eval Process (Developer Guide)

This guide describes a developer-friendly way to run retrieval/ingestion evals in `katalog` using
workflows.

## General process

### Decisions

- Eval runs are workflow-driven.
- Artifacts are saved under workspace `output` paths (using current analyzer/export conventions).
- Run name is deterministic and requires no manual label:
  - `<workflow_stem>__changeset-<changeset_id>`
- Compare runs by comparing exported CSV files.

### Recommended workspace layout

Current analyzers already write to:
- `exports/` (CSV/text files)
- `eval/latest/`
- `eval/output/eval_run_<timestamp>/`

For consistency, eval-specific outputs should keep this style and include run name in file names:
- `exports/<workflow_stem>__changeset-<id>_*.csv`
- `eval/output/<workflow_stem>__changeset-<id>/...`

### Run loop

1. Pick workflow variant (`baseline.toml`, `candidate_a.toml`, ...).
2. Run workflow:

```bash
uv run katalog -w workspace_eval workflows apply -f workflows/baseline.toml
```

3. Read the source/processor changeset ids from CLI output.
4. Run eval analyzer(s) and export CSV artifacts.
5. Repeat for next workflow variant.
6. Compare CSV outputs (`diff`, notebook, spreadsheet, script).

### Reproducibility rules

- Keep dataset fixed during comparison.
- Change one variable per workflow when possible.
- Store run metadata next to artifacts:
  - workflow file path
  - changeset id
  - git commit
  - timestamp

## Document vector search eval

This section targets:
- document parsing
- chunk generation
- vector indexing
- semantic retrieval ranking

### Existing components to reuse

- Processors:
  - `katalog.processors.eval_text_quality.EvalTextQualityProcessor`
  - `katalog.processors.eval_truth_compare.EvalTruthCompareProcessor`
- Analyzer:
  - `katalog.analyzers.eval_metrics.EvalMetricsAnalyzer`

These already produce parsing-oriented metrics and CSV exports.

### Additional metric needed for retrieval quality

Add a retrieval eval analyzer/processor that computes ranking quality with:

- `HitRate@k`
- `MRR@k`
- `Recall@k`

Use the pre-authored sidecar truth data (no per-run manual labeling).

### Truth data required

For vector search eval, each document should have sidecars prepared ahead of time:

1. `truth_text`: canonical text of document content
2. `queries`: list of eval queries
3. `quote`: expected supporting snippet per query
4. optional `answer`: expected answer text

In current sidecar style this maps naturally to:
- `.truth.md` (document truth text)
- `.queries.yml` (query + quote pairs)
- optional `.summary.md`

Recommended `.queries.yml` shape:

```yaml
queries:
  - id: q1
    query: "How do I reset my password?"
    quote: "Click Forgot Password on the login screen and follow the email link."
    answer: "Use Forgot Password and confirm via email."
  - id: q2
    query: "Where can I find billing settings?"
    quote: "Billing settings are available under Account > Subscription."
```

Rules:
- `query` and `quote` are required for search eval.
- Keep `quote` short and exact (or near-exact) so matching is stable.
- One query can have multiple acceptable quotes later, but start with one.

### How truth maps into pipeline

- Source/sidecar stage loads truth/query sidecars.
- Processors write truth/query metadata keys (already supported by eval processors).
- Retrieval eval analyzer reads:
  - chunk metadata (`document/chunk_text`)
  - query/quote truth metadata
  - vector search results
- Relevance rule:
  - a hit is relevant when returned chunk text contains (or fuzzy-matches) the query `quote`.
- Analyzer exports:
  - `*_retrieval_metrics.csv` (aggregates)
  - `*_retrieval_hits.csv` (per query/hit details)

### Minimal eval workflow shape

1. Source actor (e.g. GCS/filesystem)
2. `KreuzbergDocumentExtractProcessor`
3. `KreuzbergVectorIndexProcessor`
4. `EvalTextQualityProcessor`
5. `EvalTruthCompareProcessor`
6. Retrieval eval analyzer (new)
7. `EvalMetricsAnalyzer`

### Practical compare flow

- Run baseline workflow -> capture changeset id `A`.
- Run candidate workflow -> capture changeset id `B`.
- Compare:
  - `<workflow_baseline>__changeset-A_*`
  - `<workflow_candidate>__changeset-B_*`

This gives a stable, no-manual-label eval loop.

### Interpreting results

Compare metrics between workflow variants on the same dataset:

- `HitRate@5`: higher is better for “did user get any useful hit quickly”.
- `MRR@10`: higher is better for “how early first relevant hit appears”.
- `Recall@10`: higher is better for “how much relevant support was retrieved”.

Practical decision pattern:

1. Reject variants with lower `HitRate@5` unless there is a strong reason.
2. Between close `HitRate@5` variants, prefer higher `MRR@10`.
3. Use `Recall@10` as tie-breaker for richer context retrieval.
4. Keep latency and index size as operational guardrails.
