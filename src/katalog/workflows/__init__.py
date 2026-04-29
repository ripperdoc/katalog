from __future__ import annotations

from importlib import import_module
from typing import Any

_RUNTIME_EXPORTS = {
    "discover_workflow_files",
    "start_workflow_file",
    "load_workflow_spec",
    "load_workflow_specs",
    "run_workflow_file",
    "sync_workflow_file",
    "workflow_status",
}

_SPECS_EXPORTS = {
    "WorkflowActorSpec",
    "WorkflowSpec",
}

_RESULTS_EXPORTS = {
    "WorkflowChangesetResult",
    "WorkflowRunResult",
}

__all__ = sorted(_RUNTIME_EXPORTS | _SPECS_EXPORTS | _RESULTS_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _RUNTIME_EXPORTS:
        module = import_module(".runtime", __name__)
        return getattr(module, name)
    if name in _SPECS_EXPORTS:
        module = import_module(".specs", __name__)
        return getattr(module, name)
    if name in _RESULTS_EXPORTS:
        module = import_module(".results", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
