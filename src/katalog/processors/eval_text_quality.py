from __future__ import annotations

import re
from typing import FrozenSet

from katalog.constants.metadata import (
    DOC_TEXT,
    EVAL_AVG_SENTENCE_WORDS,
    EVAL_SENTENCE_COUNT,
    EVAL_UNIQUE_WORD_RATIO,
    MetadataKey,
)
from katalog.models import MetadataChanges, OpStatus, make_metadata
from katalog.processors.base import Processor, ProcessorResult

_WORD_RE = re.compile(r"\w+", flags=re.UNICODE)
_SENTENCE_RE = re.compile(r"[.!?]+")


class EvalTextQualityProcessor(Processor):
    plugin_id = "katalog.processors.eval_text_quality.EvalTextQualityProcessor"
    title = "Eval text quality"
    description = "Compute parser quality metrics from extracted text."
    execution_mode = "cpu"
    _dependencies = frozenset({DOC_TEXT})
    _outputs = frozenset(
        {
            EVAL_SENTENCE_COUNT,
            EVAL_AVG_SENTENCE_WORDS,
            EVAL_UNIQUE_WORD_RATIO,
        }
    )

    @property
    def dependencies(self) -> FrozenSet[MetadataKey]:
        return self._dependencies

    @property
    def outputs(self) -> FrozenSet[MetadataKey]:
        return self._outputs

    def should_run(self, changes: MetadataChanges) -> bool:
        changed_keys = changes.changed_keys()
        if DOC_TEXT in changed_keys:
            return True
        current = changes.current()
        return any(key not in current for key in self.outputs)

    async def run(self, changes: MetadataChanges) -> ProcessorResult:
        text = _latest_text(changes, DOC_TEXT)
        if not text:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="No extracted document text"
            )

        words = _WORD_RE.findall(text.lower())
        if not words:
            return ProcessorResult(
                status=OpStatus.SKIPPED, message="Document text contains no words"
            )
        sentence_count = _estimate_sentence_count(text)
        total_words = len(words)
        unique_word_ratio = len(set(words)) / total_words
        avg_sentence_words = total_words / max(1, sentence_count)

        return ProcessorResult(
            metadata=[
                make_metadata(EVAL_SENTENCE_COUNT, sentence_count, self.actor.id),
                make_metadata(EVAL_AVG_SENTENCE_WORDS, avg_sentence_words, self.actor.id),
                make_metadata(EVAL_UNIQUE_WORD_RATIO, unique_word_ratio, self.actor.id),
            ]
        )


def _latest_text(changes: MetadataChanges, key: MetadataKey) -> str:
    entries = changes.current().get(key, [])
    for entry in entries:
        value = entry.value
        if isinstance(value, str) and value:
            return value
    return ""


def _estimate_sentence_count(text: str) -> int:
    rough = len([s for s in _SENTENCE_RE.split(text) if s.strip()])
    if rough > 0:
        return rough
    return 1
