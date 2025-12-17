from __future__ import annotations

from katalog.analyzers.base import AnalyzerResult, make_analyzer_instance
from katalog.models import Provider, ProviderType


async def run_analyzers(ids: list[int] | None) -> dict[str, AnalyzerResult]:
    providers = await Provider.filter(type=ProviderType.ANALYZER).order_by("id")
    if not providers:
        raise ValueError("No analyzer providers found")
    for provider in providers:
        analyzer = make_analyzer_instance(provider)
    raise NotImplementedError()
