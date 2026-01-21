from __future__ import annotations

from dataclasses import asdict

from katalog.analyzers.base import AnalyzerResult, make_analyzer_instance
from katalog.models import Provider, ProviderType, Changeset


async def run_analyzers(ids: list[int] | None = None) -> list[dict]:
    """Run selected or all analyzers and return serialized results."""

    query = Provider.filter(type=ProviderType.ANALYZER).order_by("id")
    if ids:
        query = query.filter(id__in=sorted(set(ids)))
    providers = await query
    if not providers:
        raise ValueError("No analyzer providers found")

    results: list[dict] = []
    for provider in providers:
        analyzer = make_analyzer_instance(provider)
        async with Changeset.context(provider=provider) as changeset:
            if not analyzer.should_run(changeset=changeset):
                continue
            result = await analyzer.run(changeset=changeset)
        results.append(
            {
                "provider_id": provider.id,
                "name": provider.name,
                "plugin_id": provider.plugin_id,
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
