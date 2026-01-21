from __future__ import annotations

from dataclasses import asdict

from katalog.analyzers.base import AnalyzerResult, make_analyzer_instance
from katalog.models import Actor, ActorType, Changeset


async def run_analyzers(ids: list[int] | None = None) -> list[dict]:
    """Run selected or all analyzers and return serialized results."""

    query = Actor.filter(type=ActorType.ANALYZER).order_by("id")
    if ids:
        query = query.filter(id__in=sorted(set(ids)))
    actors = await query
    if not actors:
        raise ValueError("No analyzer actors found")

    results: list[dict] = []
    for actor in actors:
        analyzer = make_analyzer_instance(actor)
        async with Changeset.context(actor=actor) as changeset:
            if not analyzer.should_run(changeset=changeset):
                continue
            result = await analyzer.run(changeset=changeset)
        results.append(
            {
                "actor_id": actor.id,
                "name": actor.name,
                "plugin_id": actor.plugin_id,
                "result": serialize_result(result),
            }
        )
    return results


def serialize_result(result: AnalyzerResult) -> dict:
    """Convert AnalyzerResult dataclasses into JSON-serializable structures."""

    return {
        "metadata": [m.to_dict() for m in result.metadata],
        "relationships": [asdict(rel) for rel in result.relationships],
        "groups": [asdict(group) for group in result.groups],
        "issues": [asdict(issue) for issue in result.issues],
    }
