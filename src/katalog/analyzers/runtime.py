from __future__ import annotations

from katalog.analyzers.base import AnalyzerResult, AnalyzerScope, make_analyzer_instance
from katalog.models import Actor, Changeset


async def run_analyzer(
    actor: Actor,
    changeset: Changeset,
    scope: AnalyzerScope | None = None,
) -> dict:
    """Run a specific analyzer (by actor instance) and return serialized results."""

    if actor.disabled:
        raise ValueError("Analyzer actor disabled")

    analyzer = await make_analyzer_instance(actor)
    resolved_scope = scope or AnalyzerScope.all()
    if resolved_scope.kind == "collection" and not analyzer.supports_collection:
        raise ValueError("Analyzer does not support collection scope")
    if resolved_scope.kind == "asset" and not analyzer.supports_single_asset:
        raise ValueError("Analyzer does not support single-asset scope")
    if resolved_scope.kind == "all" and not analyzer.supports_all:
        raise ValueError("Analyzer does not support full-dataset scope")
    result: AnalyzerResult
    if not analyzer.should_run(changeset=changeset):
        # TODO mark as skipped
        result = AnalyzerResult()
    else:
        result = await analyzer.run(changeset=changeset, scope=resolved_scope)

    if result.output is not None:
        data_payload = dict(changeset.data or {})
        outputs = dict(data_payload.get("outputs") or {})
        outputs[str(actor.id)] = {
            "plugin_id": actor.plugin_id,
            "kind": analyzer.output_kind or "analysis",
            "scope": resolved_scope.to_dict(),
            "data": result.output,
        }
        data_payload["outputs"] = outputs
        changeset.data = data_payload
        await changeset.save(update_fields=["data"])

    return result.to_dict()
