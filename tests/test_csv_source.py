from __future__ import annotations

import csv

import pytest

from katalog.constants.metadata import ASSET_ACTOR_ID, FILE_SIZE, FILE_TITLE
from katalog.db.actors import get_actor_repo
from katalog.db.assets import get_asset_repo
from katalog.db.changesets import get_changeset_repo
from katalog.db.metadata import get_metadata_repo
from katalog.models import ActorType, MetadataChanges, OpStatus
from katalog.plugins.registry import get_actor_instance
from katalog.sources.runtime import run_sources
from katalog.sources.tabular import TABULAR_ROW_NUMBER


@pytest.mark.asyncio
async def test_csv_source_scans_local_file_with_header_row_and_mappings(
    db_session, tmp_path
) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    csv_file = tmp_path / "products.csv"
    with csv_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Report generated", "2026-04-22"])
        writer.writerow(["product_id", "name", "size"])
        writer.writerow(["SKU-001", "Widget", "12"])
        writer.writerow(["SKU-002", "Gadget", "7"])

    csv_actor = await actor_db.create(
        name="csv-products",
        plugin_id="katalog.sources.csv.CsvSource",
        type=ActorType.SOURCE,
        config={
            "csv_file": str(csv_file),
            "namespace": "products",
            "id_column": "product_id",
            "header_row": 2,
            "columns": [
                {
                    "column": "name",
                    "key": str(FILE_TITLE),
                    "value_type": "string",
                },
                {
                    "column": "size",
                    "key": str(FILE_SIZE),
                    "value_type": "int",
                },
            ],
        },
    )

    changeset = await changeset_db.begin(
        actors=[csv_actor],
        message="csv scan",
        status=OpStatus.IN_PROGRESS,
    )
    status = await run_sources(
        sources=[csv_actor],
        changeset=changeset,
        run_processors=False,
    )
    await changeset.finalize(status=status)

    assert status == OpStatus.COMPLETED

    assets = await asset_db.list_rows(order_by="id")
    assert [asset.external_id for asset in assets] == ["SKU-001", "SKU-002"]

    first_asset_metadata = await md_db.for_asset(assets[0], include_removed=True)
    second_asset_metadata = await md_db.for_asset(assets[1], include_removed=True)
    first_changes = MetadataChanges(asset=assets[0], loaded=first_asset_metadata)
    second_changes = MetadataChanges(asset=assets[1], loaded=second_asset_metadata)

    assert first_changes.latest_value(FILE_TITLE, value_type=str) == "Widget"
    assert second_changes.latest_value(FILE_TITLE, value_type=str) == "Gadget"
    assert first_changes.latest_value(FILE_SIZE, value_type=int) == 12
    assert second_changes.latest_value(FILE_SIZE, value_type=int) == 7
    assert first_changes.latest_value(TABULAR_ROW_NUMBER, value_type=int) == 3
    assert second_changes.latest_value(TABULAR_ROW_NUMBER, value_type=int) == 4


@pytest.mark.asyncio
async def test_csv_source_falls_back_to_row_number_when_id_column_is_missing(
    db_session, tmp_path
) -> None:
    _ = db_session
    actor_db = get_actor_repo()
    changeset_db = get_changeset_repo()
    asset_db = get_asset_repo()
    md_db = get_metadata_repo()

    csv_file = tmp_path / "products-no-id.csv"
    with csv_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["name", "size"])
        writer.writerow(["Widget", "12"])
        writer.writerow(["Gadget", "7"])

    csv_actor = await actor_db.create(
        name="csv-products-row-number-id",
        plugin_id="katalog.sources.csv.CsvSource",
        type=ActorType.SOURCE,
        config={
            "csv_file": str(csv_file),
            "namespace": "products-row-number-id",
            "columns": [
                {
                    "column": "name",
                    "key": str(FILE_TITLE),
                    "value_type": "string",
                },
                {
                    "column": "size",
                    "key": str(FILE_SIZE),
                    "value_type": "int",
                },
            ],
        },
    )

    changeset = await changeset_db.begin(
        actors=[csv_actor],
        message="csv scan row-number id",
        status=OpStatus.IN_PROGRESS,
    )
    status = await run_sources(
        sources=[csv_actor],
        changeset=changeset,
        run_processors=False,
    )
    await changeset.finalize(status=status)

    assert status == OpStatus.COMPLETED

    assets = await asset_db.list_rows(order_by="id")
    assert [asset.external_id for asset in assets] == ["2", "3"]

    first_asset_metadata = await md_db.for_asset(assets[0], include_removed=True)
    second_asset_metadata = await md_db.for_asset(assets[1], include_removed=True)
    first_changes = MetadataChanges(asset=assets[0], loaded=first_asset_metadata)
    second_changes = MetadataChanges(asset=assets[1], loaded=second_asset_metadata)

    assert first_changes.latest_value(FILE_TITLE, value_type=str) == "Widget"
    assert second_changes.latest_value(FILE_TITLE, value_type=str) == "Gadget"
    assert first_changes.latest_value(TABULAR_ROW_NUMBER, value_type=int) == 2
    assert second_changes.latest_value(TABULAR_ROW_NUMBER, value_type=int) == 3


@pytest.mark.asyncio
async def test_csv_source_view_definitions_show_actor_id_column(
    db_session, tmp_path
) -> None:
    _ = db_session
    actor_db = get_actor_repo()

    csv_file = tmp_path / "products-view.csv"
    with csv_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["product_id", "name"])
        writer.writerow(["SKU-001", "Widget"])

    csv_actor = await actor_db.create(
        name="csv-products-view",
        plugin_id="katalog.sources.csv.CsvSource",
        type=ActorType.SOURCE,
        config={
            "csv_file": str(csv_file),
            "namespace": "products-view",
            "id_column": "product_id",
            "columns": [
                {
                    "column": "name",
                    "key": str(FILE_TITLE),
                    "value_type": "string",
                }
            ],
        },
    )

    source = await get_actor_instance(csv_actor)
    views = source.view_definitions()

    assert len(views) == 1
    actor_column = views[0].column_map()[str(ASSET_ACTOR_ID)]
    assert actor_column.hidden is False
