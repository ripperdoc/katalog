from __future__ import annotations

from katalog.analyzers.base import AnalyzerResult, AnalyzerScope, make_analyzer_instance
from katalog.db.assets import get_asset_repo
from katalog.models import Actor, Changeset
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import MetadataChanges


async def do_run_analyzer(
    actor: Actor,
    changeset: Changeset,
    scope: AnalyzerScope | None = None,
) -> AnalyzerResult:
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

    if result.metadata:
        md_db = get_metadata_repo()
        asset_db = get_asset_repo()
        by_asset: dict[int, list] = {}
        for entry in result.metadata:
            if entry.asset_id is None:
                continue
            if entry.actor_id is None:
                entry.actor_id = actor.id
            if entry.changeset_id is None:
                entry.changeset_id = changeset.id
            by_asset.setdefault(int(entry.asset_id), []).append(entry)
        for asset_id, staged in by_asset.items():
            asset = await asset_db.get_or_none(id=asset_id)
            if asset is None:
                continue
            loaded = await asset_db.load_metadata(asset, include_removed=True)
            changes = MetadataChanges(asset=asset, loaded=list(loaded), staged=staged)
            await md_db.persist_changes(changes, changeset=changeset)

    if result.output is not None:
        data_payload = dict(changeset.data or {})
        outputs = dict(data_payload.get("outputs") or {})
        outputs[str(actor.id)] = {
            "plugin_id": actor.plugin_id,
            "kind": analyzer.output_kind or "analysis",
            "scope": resolved_scope.model_dump(mode="json"),
            "data": result.output,
        }
        data_payload["outputs"] = outputs
        changeset.data = data_payload
        db = get_changeset_repo()
        await db.save(changeset, update_data=changeset.data)

    return result
