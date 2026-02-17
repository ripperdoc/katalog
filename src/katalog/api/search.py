from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from katalog.constants.metadata import get_metadata_def_by_id, get_metadata_id
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.vectors import VectorSearchHit, get_vector_repo
from katalog.models.query import AssetQuery
from katalog.models import ActorType
from katalog.api.helpers import ApiError
from katalog.vectors.embedding import embed_text_kreuzberg


@dataclass(frozen=True)
class SemanticHit:
    asset_id: int
    metadata_id: int | None
    metadata_key_id: int
    metadata_key: str
    text: str
    distance: float
    score: float


async def semantic_hits_for_query(query: AssetQuery) -> tuple[list[SemanticHit], int, int]:
    started = perf_counter()
    if query.search_mode not in {"semantic", "hybrid"}:
        return [], 0, 0
    search_text = (query.search or "").strip()
    if not search_text:
        raise ApiError(status_code=400, detail="search is required for semantic mode")

    vec_db = get_vector_repo()
    ready, reason = await vec_db.is_ready()
    if not ready:
        raise ApiError(
            status_code=409,
            detail=f"Vector search is not ready: {reason or 'unknown reason'}",
        )

    asset_db = get_asset_repo()
    scope_asset_ids = await _scope_asset_ids(query)

    vector_actor_id = await _resolve_vector_actor_id(query.search_index)
    top_k = query.search_top_k or max(50, query.limit * 5)
    query_vector = await embed_text_kreuzberg(
        search_text,
        model=query.search_embedding_model,
        backend=query.search_embedding_backend,
        dim=int(query.search_dimension),
    )
    raw_hits = await vec_db.search(
        actor_id=vector_actor_id,
        dim=int(query.search_dimension),
        query_vector=query_vector,
        limit=top_k,
        asset_ids=scope_asset_ids,
    )
    filtered = _filter_hits(
        raw_hits,
        metadata_keys=query.search_metadata_keys,
        min_score=query.search_min_score,
    )
    elapsed_ms = int((perf_counter() - started) * 1000)
    return filtered, len(filtered), elapsed_ms


async def _resolve_vector_actor_id(search_index: int | None) -> int:
    if search_index is not None:
        return int(search_index)
    actor_db = get_actor_repo()
    actors = await actor_db.list_rows(
        type=ActorType.PROCESSOR,
        plugin_id="katalog.processors.vector_index.KreuzbergVectorIndexProcessor",
        disabled=0,
        order_by="id ASC",
    )
    if len(actors) == 1 and actors[0].id is not None:
        return int(actors[0].id)
    if not actors:
        raise ApiError(
            status_code=400,
            detail="No vector index actor found. Configure and run a vector index processor first.",
        )
    raise ApiError(
        status_code=400,
        detail="Multiple vector index actors found; specify search_index as actor id.",
    )


async def _scope_asset_ids(query: AssetQuery) -> list[int] | None:
    has_scope = bool(query.filters)
    if not has_scope:
        return None
    asset_db = get_asset_repo()
    scoped_query = query.model_copy(
        update={
            "search": None,
            "search_mode": "fts",
            "search_granularity": "asset",
            "offset": 0,
            "limit": 1000000,
            "sort": None,
            "group_by": None,
        }
    )
    return await asset_db.list_asset_ids_for_query(query=scoped_query)


def _filter_hits(
    hits: list[VectorSearchHit],
    *,
    metadata_keys: list[str] | None,
    min_score: float | None,
) -> list[SemanticHit]:
    key_ids: set[int] | None = None
    if metadata_keys:
        key_ids = {int(get_metadata_id(key)) for key in metadata_keys}

    filtered: list[SemanticHit] = []
    for hit in hits:
        if key_ids is not None and hit.metadata_key_id not in key_ids:
            continue
        score = 1.0 / (1.0 + max(0.0, float(hit.distance)))
        if min_score is not None and score < float(min_score):
            continue
        try:
            key = str(get_metadata_def_by_id(hit.metadata_key_id).key)
        except Exception:  # noqa: BLE001
            key = str(hit.metadata_key_id)
        filtered.append(
            SemanticHit(
                asset_id=int(hit.asset_id),
                metadata_id=hit.metadata_id,
                metadata_key_id=int(hit.metadata_key_id),
                metadata_key=key,
                text=hit.source_text,
                distance=float(hit.distance),
                score=score,
            )
        )
    return filtered
