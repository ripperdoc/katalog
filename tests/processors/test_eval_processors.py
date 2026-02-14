from __future__ import annotations

import pytest

from katalog.constants.metadata import (
    DOC_TEXT,
    EVAL_AVG_SENTENCE_WORDS,
    EVAL_COMPLETENESS,
    EVAL_SENTENCE_COUNT,
    EVAL_SIMILARITY,
    EVAL_TRUTH_TEXT,
    EVAL_UNIQUE_WORD_RATIO,
)
from katalog.models import Actor, ActorType, Asset, MetadataChanges, OpStatus, make_metadata
from katalog.processors.eval_text_quality import EvalTextQualityProcessor
from katalog.processors.eval_truth_compare import EvalTruthCompareProcessor


def _asset() -> Asset:
    return Asset(
        id=1,
        namespace="web",
        external_id="https://example.com/doc",
        canonical_uri="https://example.com/doc",
        actor_id=1,
    )


def _processor_actor() -> Actor:
    return Actor(
        id=99,
        name="eval-proc",
        plugin_id="katalog.processors.eval_truth_compare.EvalTruthCompareProcessor",
        type=ActorType.PROCESSOR,
    )


@pytest.mark.asyncio
async def test_eval_truth_compare_outputs_similarity_and_completeness(db_session) -> None:
    _ = db_session
    actor = _processor_actor()
    processor = EvalTruthCompareProcessor(actor=actor)
    changes = MetadataChanges(
        asset=_asset(),
        loaded=[],
        staged=[
            make_metadata(DOC_TEXT, "one two three", actor_id=actor.id),
            make_metadata(EVAL_TRUTH_TEXT, "one two", actor_id=actor.id),
        ],
    )

    result = await processor.run(changes)

    assert result.status == OpStatus.COMPLETED
    keys = {str(md.key) for md in result.metadata}
    assert str(EVAL_SIMILARITY) in keys
    assert str(EVAL_COMPLETENESS) in keys


@pytest.mark.asyncio
async def test_eval_text_quality_outputs_basic_metrics(db_session) -> None:
    _ = db_session
    actor = _processor_actor()
    processor = EvalTextQualityProcessor(actor=actor)
    changes = MetadataChanges(
        asset=_asset(),
        loaded=[],
        staged=[make_metadata(DOC_TEXT, "Hello world. Hello tests.", actor_id=actor.id)],
    )

    result = await processor.run(changes)

    assert result.status == OpStatus.COMPLETED
    keys = {str(md.key) for md in result.metadata}
    assert str(EVAL_SENTENCE_COUNT) in keys
    assert str(EVAL_AVG_SENTENCE_WORDS) in keys
    assert str(EVAL_UNIQUE_WORD_RATIO) in keys
