import csv

import pytest

from katalog.api.views import _plugin_views_for_actor
from katalog.constants.metadata import (
    ASSET_ACTOR_ID,
    ASSET_EXTERNAL_ID,
    ASSET_ID,
    FILE_TITLE,
)
from katalog.db.actors import get_actor_repo
from katalog.models import ActorType
from katalog.models.views import ColumnSpec, ViewSpec, default_view, ensure_actor_column


def test_default_view_is_named_files() -> None:
    view = default_view()

    assert view.id == "default"
    assert view.name == "Files"


def test_ensure_actor_column_inserts_actor_column_near_front() -> None:
    view = ViewSpec(
        id="generated",
        name="Generated",
        columns=[
            ColumnSpec.from_metadata(ASSET_ID, sortable=True, width=80),
            ColumnSpec.from_metadata(ASSET_EXTERNAL_ID, searchable=True),
        ],
        default_sort=[(str(ASSET_ID), "asc")],
        default_columns=[str(ASSET_EXTERNAL_ID)],
    )

    normalized = ensure_actor_column(view)

    assert [column.id for column in normalized.columns[:3]] == [
        str(ASSET_ID),
        str(ASSET_ACTOR_ID),
        str(ASSET_EXTERNAL_ID),
    ]
    assert normalized.default_columns is not None
    assert list(normalized.default_columns[:2]) == [
        str(ASSET_ACTOR_ID),
        str(ASSET_EXTERNAL_ID),
    ]


@pytest.mark.asyncio
async def test_runtime_views_use_url_safe_ids(db_session, tmp_path) -> None:
    _ = db_session
    actor_db = get_actor_repo()

    csv_file = tmp_path / "products-view-id.csv"
    with csv_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["product_id", "name"])
        writer.writerow(["SKU-001", "Widget"])

    csv_actor = await actor_db.create(
        name="csv-products-view-id",
        plugin_id="katalog.sources.csv.CsvSource",
        type=ActorType.SOURCE,
        config={
            "csv_file": str(csv_file),
            "namespace": "products-view-id",
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

    views = await _plugin_views_for_actor(csv_actor)

    assert [view.id for view in views] == [f"actor-{csv_actor.id}-table"]
