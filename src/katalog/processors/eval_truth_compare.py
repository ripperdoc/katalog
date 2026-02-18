from __future__ import annotations

import difflib
import re
from typing import FrozenSet

from katalog.constants.metadata import (
    DOC_TEXT,
    EVAL_COMPLETENESS,
    EVAL_SIMILARITY,
    EVAL_TRUTH_TEXT,
    MetadataKey,
)
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult

_WORD_RE = re.compile(r"\w+", flags=re.UNICODE)


class EvalTruthCompareProcessor(Processor):
    plugin_id = "katalog.processors.eval_truth_compare.EvalTruthCompareProcessor"
    title = "Eval truth compare"
    description = "Compare extracted text to eval ground truth text."
    execution_mode = "cpu"
    _dependencies = frozenset({DOC_TEXT, EVAL_TRUTH_TEXT})
    _outputs = frozenset({EVAL_SIMILARITY, EVAL_COMPLETENESS})

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return self._outputs

    def should_run(self, changes: MetadataChanges) -> bool:
        changed_keys = changes.changed_keys()
        if DOC_TEXT in changed_keys or EVAL_TRUTH_TEXT in changed_keys:
            return True
        current = changes.current()
        return any(key not in current for key in self.outputs)

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        text = _latest_text(changes, DOC_TEXT)
        truth = _latest_text(changes, EVAL_TRUTH_TEXT)
        if not text:
            return ProcessorResult(status=OpStatus.SKIPPED, message="No extracted text")
        if not truth:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="No eval truth text configured"
            )

        similarity = difflib.SequenceMatcher(None, truth, text).ratio() * 100.0
        completeness = _completeness(truth, text)

        return ProcessorResult(
            metadata=[
                make_metadata(EVAL_SIMILARITY, similarity, self.actor.id),
                make_metadata(EVAL_COMPLETENESS, completeness, self.actor.id),
            ]
        )


def _latest_text(changes: MetadataChanges, key: MetadataKey) -> str:
    value = changes.latest_value(key, value_type=str)
    if value:
        return value
    return ""


def _completeness(truth: str, text: str) -> float:
    truth_words = set(_WORD_RE.findall(truth.lower()))
    if not truth_words:
        return 0.0
    text_words = set(_WORD_RE.findall(text.lower()))
    overlap = truth_words.intersection(text_words)
    return (len(overlap) / len(truth_words)) * 100.0
