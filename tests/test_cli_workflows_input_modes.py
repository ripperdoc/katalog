from __future__ import annotations

import pytest
import typer

from katalog.cli.workflows import _build_cli_workflow_input
from katalog.workflows.contracts import (
    WorkflowAllAssetsInput,
    WorkflowAssetIdsInput,
    WorkflowCollectionInput,
    WorkflowSourceActorsInput,
)


def test_cli_input_override_none_returns_none() -> None:
    assert (
        _build_cli_workflow_input(
            input_all=False,
            input_actor=[],
            input_collection=None,
            input_asset=[],
        )
        is None
    )


def test_cli_input_override_all_assets() -> None:
    workflow_input = _build_cli_workflow_input(
        input_all=True,
        input_actor=[],
        input_collection=None,
        input_asset=[],
    )
    assert isinstance(workflow_input, WorkflowAllAssetsInput)


def test_cli_input_override_source_actors_deduplicates_values() -> None:
    workflow_input = _build_cli_workflow_input(
        input_all=False,
        input_actor=[7, 2, 7],
        input_collection=None,
        input_asset=[],
    )
    assert isinstance(workflow_input, WorkflowSourceActorsInput)
    assert workflow_input.actor_ids == [2, 7]


def test_cli_input_override_collection() -> None:
    workflow_input = _build_cli_workflow_input(
        input_all=False,
        input_actor=[],
        input_collection=12,
        input_asset=[],
    )
    assert isinstance(workflow_input, WorkflowCollectionInput)
    assert workflow_input.collection_id == 12


def test_cli_input_override_asset_ids_deduplicates_values() -> None:
    workflow_input = _build_cli_workflow_input(
        input_all=False,
        input_actor=[],
        input_collection=None,
        input_asset=[9, 4, 9],
    )
    assert isinstance(workflow_input, WorkflowAssetIdsInput)
    assert workflow_input.asset_ids == [4, 9]


@pytest.mark.parametrize(
    ("input_all", "input_actor", "input_collection", "input_asset"),
    [
        (True, [1], None, []),
        (True, [], 2, []),
        (True, [], None, [3]),
        (False, [1], 2, []),
        (False, [1], None, [3]),
        (False, [], 2, [3]),
    ],
)
def test_cli_input_override_rejects_mixed_selector_kinds(
    input_all: bool,
    input_actor: list[int],
    input_collection: int | None,
    input_asset: list[int],
) -> None:
    with pytest.raises(typer.BadParameter):
        _build_cli_workflow_input(
            input_all=input_all,
            input_actor=input_actor,
            input_collection=input_collection,
            input_asset=input_asset,
        )
