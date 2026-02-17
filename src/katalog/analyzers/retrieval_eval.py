from __future__ import annotations

import json
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field

from katalog.api.assets import list_assets as list_assets_api
from katalog.api.metadata import list_metadata as list_metadata_api
from katalog.analyzers.base import Analyzer, AnalyzerResult, AnalyzerScope
from katalog.constants.metadata import (
    DOC_CHUNK_TEXT,
    EVAL_QUERIES,
    SIDECAR_TYPE,
    get_metadata_def_by_id,
    get_metadata_id,
)
from katalog.db.assets import get_asset_repo
from katalog.db.vectors import VectorSearchHit, get_vector_repo
from katalog.models import Actor, Changeset
from katalog.models.query import AssetFilter, AssetQuery
from katalog.utils.exports import analyzer_export_dir, write_csv_tables
from katalog.vectors.embedding import embed_text_kreuzberg


@dataclass(frozen=True)
class QueryCase:
    case_id: str
    source_asset_id: int
    query: str
    quote: str | None
    relevant_metadata_ids: tuple[int, ...]
    relevant_asset_ids: tuple[int, ...]
    relevant_asset_external_ids: tuple[str, ...]
    relevant_asset_uris: tuple[str, ...]


class RetrievalEvalAnalyzer(Analyzer):
    plugin_id = "katalog.analyzers.retrieval_eval.RetrievalEvalAnalyzer"
    title = "Retrieval eval"
    description = "Evaluate semantic retrieval using HitRate@k, MRR@k, and Recall@k."
    output_kind = "retrieval_eval"
    supports_single_asset = False

    class ConfigModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

        k_values: list[int] = Field(default_factory=lambda: [5, 10])
        search_index: int
        metadata_keys: list[str] = Field(default_factory=lambda: [str(DOC_CHUNK_TEXT)])
        query_metadata_key: str = str(EVAL_QUERIES)
        top_k: int = Field(
            default=100,
            gt=0,
            description="Vector candidate pool size per eval query.",
        )
        min_score: float | None = Field(default=None, ge=0.0, le=1.0)
        search_dimension: int = Field(default=64, gt=0)
        embedding_model: str = "fast"
        embedding_backend: str = "preset"
        max_queries: int = Field(
            default=0,
            ge=0,
            description="0 means evaluate all query cases.",
        )

    config_model = ConfigModel

    def __init__(self, actor: Actor, **config: Any) -> None:
        self.config = self.config_model.model_validate(config or {})
        super().__init__(actor, **config)

    def should_run(self, *, changeset: Changeset) -> bool:
        _ = changeset
        return True

    async def run(self, *, changeset: Changeset, scope: AnalyzerScope) -> AnalyzerResult:
        if scope.kind == "asset":
            raise ValueError("Retrieval eval analyzer does not support single-asset scope")

        start = perf_counter()
        scoped_asset_ids = await self._resolve_scoped_asset_ids(scope)
        if not scoped_asset_ids:
            return AnalyzerResult(
                output={
                    "summary": {
                        "queries_total": 0,
                        "queries_evaluated": 0,
                        "k_values": self._k_values(),
                        "metrics": {},
                        "duration_ms": int((perf_counter() - start) * 1000),
                    }
                }
            )

        query_cases = await self._load_query_cases(scope)
        if self.config.max_queries > 0:
            query_cases = query_cases[: int(self.config.max_queries)]

        vec_db = get_vector_repo()
        ready, reason = await vec_db.is_ready()
        if not ready:
            raise RuntimeError(f"Vector search is not ready: {reason or 'unknown reason'}")

        key_ids = [int(get_metadata_id(key)) for key in self.config.metadata_keys]
        max_k = max(self._k_values())
        search_limit = max(int(self.config.top_k), max_k)
        metrics_acc = {k: {"hit": 0.0, "mrr": 0.0, "recall": 0.0} for k in self._k_values()}
        rows: list[dict[str, Any]] = []

        assets_by_id = await self._load_assets_by_id(scoped_asset_ids)
        query_count = 0
        for case in query_cases:
            query_count += 1
            query_vector = await embed_text_kreuzberg(
                case.query,
                model=self.config.embedding_model,
                backend=self.config.embedding_backend,
                dim=int(self.config.search_dimension),
            )
            raw_hits = await vec_db.search(
                actor_id=int(self.config.search_index),
                dim=int(self.config.search_dimension),
                query_vector=query_vector,
                limit=search_limit,
                asset_ids=scoped_asset_ids,
            )
            hits = self._filter_hits(raw_hits, key_ids)
            relevant_ranks = self._relevant_ranks(case=case, hits=hits, assets_by_id=assets_by_id)
            total_relevant = await self._count_total_relevant(
                case=case,
                scope=scope,
                scoped_asset_ids=scoped_asset_ids,
                key_ids=key_ids,
            )

            first_rank = min(relevant_ranks) if relevant_ranks else None
            for k in self._k_values():
                in_top_k = [rank for rank in relevant_ranks if rank <= k]
                hit_rate = 1.0 if in_top_k else 0.0
                mrr = (1.0 / float(first_rank)) if first_rank is not None and first_rank <= k else 0.0
                recall = (
                    float(len(in_top_k)) / float(total_relevant)
                    if total_relevant > 0
                    else 0.0
                )
                metrics_acc[k]["hit"] += hit_rate
                metrics_acc[k]["mrr"] += mrr
                metrics_acc[k]["recall"] += recall

            top_hit = hits[0] if hits else None
            rows.append(
                {
                    "query_id": case.case_id,
                    "query_asset_id": case.source_asset_id,
                    "query": case.query,
                    "quote": case.quote or "",
                    "first_relevant_rank": first_rank or "",
                    "total_relevant": total_relevant,
                    "top_hit_asset_id": top_hit.asset_id if top_hit is not None else "",
                    "top_hit_metadata_id": top_hit.metadata_id if top_hit is not None else "",
                    "top_hit_distance": top_hit.distance if top_hit is not None else "",
                    "top_hit_text": top_hit.source_text if top_hit is not None else "",
                }
            )

        summary_metrics: dict[str, float] = {}
        for k in self._k_values():
            denom = float(query_count) if query_count > 0 else 1.0
            summary_metrics[f"hit_rate@{k}"] = metrics_acc[k]["hit"] / denom
            summary_metrics[f"mrr@{k}"] = metrics_acc[k]["mrr"] / denom
            summary_metrics[f"recall@{k}"] = metrics_acc[k]["recall"] / denom

        duration_ms = int((perf_counter() - start) * 1000)
        summary_rows = [
            {"metric": key, "value": value} for key, value in sorted(summary_metrics.items())
        ]
        summary_rows.append({"metric": "queries_evaluated", "value": query_count})
        summary_rows.append({"metric": "duration_ms", "value": duration_ms})

        export_dir = analyzer_export_dir(
            changeset_id=int(changeset.id),
            analyzer_plugin_id=self.plugin_id,
            actor_id=self.actor.id,
        )
        prefix = f"changeset-{changeset.id}_actor-{self.actor.id}_retrieval_eval"
        csv_paths = write_csv_tables(
            {"retrieval_queries": rows, "retrieval_summary": summary_rows},
            prefix=prefix,
            directory=export_dir,
        )
        summary_json_path = export_dir / f"{prefix}_summary.json"
        output = {
            "summary": {
                "queries_total": len(query_cases),
                "queries_evaluated": query_count,
                "k_values": self._k_values(),
                "metrics": summary_metrics,
                "duration_ms": duration_ms,
            }
        }
        summary_json_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        output["exports"] = {
            "base_dir": str(export_dir),
            "csv": [str(path) for path in csv_paths],
            "json": [str(summary_json_path)],
        }

        logger.info(
            "Retrieval eval completed queries={queries} duration_ms={duration}",
            queries=query_count,
            duration=duration_ms,
        )
        return AnalyzerResult(output=output)

    async def _resolve_scoped_asset_ids(self, scope: AnalyzerScope) -> list[int]:
        asset_db = get_asset_repo()
        query = self._asset_query_for_scope(scope, limit=1_000_000)
        return await asset_db.list_asset_ids_for_query(query=query)

    async def _load_query_cases(self, scope: AnalyzerScope) -> list[QueryCase]:
        query = self._asset_query_for_scope(scope, limit=1_000_000)
        query.metadata_include_linked_sidecars = True
        response = await list_assets_api(query)
        rows = response.items
        cases: list[QueryCase] = []
        for row in rows:
            payload_row = row.model_dump(by_alias=True)
            if payload_row.get(str(SIDECAR_TYPE)):
                continue
            asset_id = int(payload_row["asset/id"])
            payload = self._decode_queries_payload(
                payload_row.get(self.config.query_metadata_key),
                payload_row.get(self.config.query_metadata_key),
            )
            if payload is None:
                continue
            queries = payload.get("queries") if isinstance(payload, dict) else payload
            if not isinstance(queries, list):
                continue
            for index, entry in enumerate(queries):
                if not isinstance(entry, dict):
                    continue
                query_text = str(entry.get("query") or entry.get("text") or "").strip()
                if not query_text:
                    continue
                case_id = str(entry.get("id") or f"{asset_id}:{index + 1}")
                quote = str(entry.get("quote") or "").strip() or None
                cases.append(
                    QueryCase(
                        case_id=case_id,
                        source_asset_id=asset_id,
                        query=query_text,
                        quote=quote,
                        relevant_metadata_ids=self._as_int_tuple(
                            entry.get("relevant_metadata_ids")
                        ),
                        relevant_asset_ids=self._as_int_tuple(entry.get("relevant_asset_ids")),
                        relevant_asset_external_ids=self._as_str_tuple(
                            entry.get("relevant_asset_external_ids")
                        ),
                        relevant_asset_uris=self._as_str_tuple(entry.get("relevant_asset_uris")),
                    )
                )
        return cases

    async def _load_assets_by_id(self, asset_ids: list[int]) -> dict[int, Any]:
        if not asset_ids:
            return {}
        db = get_asset_repo()
        assets = await db.list_rows(id__in=asset_ids, order_by="id")
        out: dict[int, Any] = {}
        for asset in assets:
            if asset.id is not None:
                out[int(asset.id)] = asset
        return out

    async def _count_total_relevant(
        self,
        *,
        case: QueryCase,
        scope: AnalyzerScope,
        scoped_asset_ids: list[int],
        key_ids: list[int],
    ) -> int:
        if case.relevant_metadata_ids:
            return len(set(case.relevant_metadata_ids))
        if case.relevant_asset_ids:
            return len(set(case.relevant_asset_ids))
        if case.relevant_asset_external_ids:
            return len(set(case.relevant_asset_external_ids))
        if case.relevant_asset_uris:
            return len(set(case.relevant_asset_uris))
        if case.quote:
            return await self._count_quote_matches(
                quote=case.quote,
                scope=scope,
                scoped_asset_ids=scoped_asset_ids,
                key_ids=key_ids,
            )
        return 1

    async def _count_quote_matches(
        self,
        *,
        quote: str,
        scope: AnalyzerScope,
        scoped_asset_ids: list[int],
        key_ids: list[int],
    ) -> int:
        if not quote.strip() or not key_ids:
            return 0
        metadata_keys = [str(get_metadata_def_by_id(key_id).key) for key_id in key_ids]
        query = self._asset_query_for_scope(scope, limit=1_000_000)
        query.search_granularity = "metadata"
        query.search_metadata_keys = metadata_keys
        query.metadata_aggregation = "latest"
        result = await list_metadata_api(query)
        quote_norm = _normalize_text(quote)
        matches = 0
        for item in result.get("items", []):
            text = _normalize_text(str(item.get("text") or ""))
            if quote_norm and quote_norm in text:
                if not scoped_asset_ids or int(item.get("asset_id") or 0) in set(scoped_asset_ids):
                    matches += 1
        return matches

    def _filter_hits(self, hits: list[VectorSearchHit], key_ids: list[int]) -> list[VectorSearchHit]:
        key_id_set = set(int(value) for value in key_ids)
        out: list[VectorSearchHit] = []
        for hit in hits:
            if int(hit.metadata_key_id) not in key_id_set:
                continue
            score = 1.0 / (1.0 + max(0.0, float(hit.distance)))
            if self.config.min_score is not None and score < float(self.config.min_score):
                continue
            out.append(hit)
        return out

    def _relevant_ranks(
        self,
        *,
        case: QueryCase,
        hits: list[VectorSearchHit],
        assets_by_id: dict[int, Any],
    ) -> list[int]:
        ranks: list[int] = []
        quote_norm = _normalize_text(case.quote or "")
        for index, hit in enumerate(hits, start=1):
            if self._is_relevant(
                case=case,
                hit=hit,
                assets_by_id=assets_by_id,
                quote_norm=quote_norm,
            ):
                ranks.append(index)
        return ranks

    def _is_relevant(
        self,
        *,
        case: QueryCase,
        hit: VectorSearchHit,
        assets_by_id: dict[int, Any],
        quote_norm: str,
    ) -> bool:
        if case.relevant_metadata_ids and hit.metadata_id in set(case.relevant_metadata_ids):
            return True
        if case.relevant_asset_ids and int(hit.asset_id) in set(case.relevant_asset_ids):
            return True

        asset = assets_by_id.get(int(hit.asset_id))
        if asset is not None:
            if case.relevant_asset_external_ids and asset.external_id in set(
                case.relevant_asset_external_ids
            ):
                return True
            if case.relevant_asset_uris and asset.canonical_uri in set(case.relevant_asset_uris):
                return True

        if quote_norm:
            return quote_norm in _normalize_text(hit.source_text)

        if (
            not case.relevant_metadata_ids
            and not case.relevant_asset_ids
            and not case.relevant_asset_external_ids
            and not case.relevant_asset_uris
        ):
            return int(hit.asset_id) == int(case.source_asset_id)
        return False

    def _k_values(self) -> list[int]:
        values = sorted({int(v) for v in self.config.k_values if int(v) > 0})
        return values or [5, 10]

    @staticmethod
    def _asset_query_for_scope(scope: AnalyzerScope, *, limit: int) -> AssetQuery:
        filters: list[AssetFilter] | None = None
        if scope.kind == "asset" and scope.asset_id is not None:
            filters = [AssetFilter(key="asset/id", op="equals", value=str(int(scope.asset_id)))]
        elif scope.kind == "collection":
            if scope.collection_id is None or scope.collection_key_id is None:
                raise ValueError("collection scope requires collection_id and collection_key_id")
            key = str(get_metadata_def_by_id(int(scope.collection_key_id)).key)
            filters = [AssetFilter(key=key, op="in", values=[str(int(scope.collection_id))])]
        return AssetQuery(
            view_id="default",
            filters=filters,
            search_mode="fts",
            search_granularity="asset",
            offset=0,
            limit=limit,
            sort=None,
            group_by=None,
        )

    @staticmethod
    def _decode_queries_payload(value_json: Any, value_text: Any) -> dict[str, Any] | list[Any] | None:
        for candidate in (value_json, value_text):
            if candidate is None:
                continue
            if isinstance(candidate, (dict, list)):
                return candidate
            if isinstance(candidate, str):
                text = candidate.strip()
                if not text:
                    continue
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, (dict, list)):
                        return parsed
                except Exception:
                    continue
        return None

    @staticmethod
    def _as_int_tuple(value: Any) -> tuple[int, ...]:
        if not isinstance(value, list):
            return ()
        out: list[int] = []
        for item in value:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                continue
        return tuple(out)

    @staticmethod
    def _as_str_tuple(value: Any) -> tuple[str, ...]:
        if not isinstance(value, list):
            return ()
        out: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return tuple(out)


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").lower().split())
