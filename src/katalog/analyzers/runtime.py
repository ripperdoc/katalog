from __future__ import annotations

from katalog.analyzers.base import AnalyzerResult, make_analyzer_instance
from katalog.models import Actor, Changeset


async def run_analyzer(
    actor: Actor,
    changeset: Changeset,
) -> dict:
    """Run a specific analyzer (by actor instance) and return serialized results."""

    if actor.disabled:
        raise ValueError("Analyzer actor disabled")

    analyzer = make_analyzer_instance(actor)
    result: AnalyzerResult
    if not analyzer.should_run(changeset=changeset):
        # TODO mark as skipped
        result = AnalyzerResult()

    result = await analyzer.run(changeset=changeset)

    return result.to_dict()
