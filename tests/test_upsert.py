"""Tests for metadata upsert behavior on Asset."""

import pytest

from katalog.constants.metadata import FILE_PATH
from tests.utils.upsert_helpers import UpsertFixture, ctx, md  # noqa: F401


@pytest.mark.asyncio
async def test_upsert_adds_first_metadata_value(ctx: UpsertFixture):
    changes = await ctx.upsert(
        actor_id=0, changeset_id=1, metas=[md(FILE_PATH, "/tmp/file1")]
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_rows(FILE_PATH)
    assert len(rows) == 1
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].changeset_id == 1


@pytest.mark.asyncio
async def test_upsert_doesnt_add_duplicate(ctx: UpsertFixture):
    await ctx.add_initial(
        actor_id=0, changeset_id=1, metas=[md(FILE_PATH, "/tmp/file1")]
    )

    changes = await ctx.upsert(
        actor_id=0, changeset_id=2, metas=[md(FILE_PATH, "/tmp/file1")]
    )

    assert not changes
    rows = await ctx.fetch_rows(FILE_PATH)
    assert len(rows) == 1
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].changeset_id == 1


@pytest.mark.asyncio
async def test_upsert_different_value_adds_second_value(ctx: UpsertFixture):
    await ctx.add_initial(
        actor_id=0, changeset_id=1, metas=[md(FILE_PATH, "/tmp/file1")]
    )

    changes = await ctx.upsert(
        actor_id=1, changeset_id=2, metas=[md(FILE_PATH, "/tmp/file2")]
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_rows(FILE_PATH)
    assert len(rows) == 2
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].changeset_id == 1
    assert rows[1].value_text == "/tmp/file2"
    assert rows[1].removed is False
    assert rows[1].changeset_id == 2


@pytest.mark.asyncio
async def test_upsert_multiple_values_adds_only_new(ctx: UpsertFixture):
    await ctx.add_initial(
        actor_id=0, changeset_id=1, metas=[md(FILE_PATH, "/tmp/file1")]
    )
    await ctx.add_initial(
        actor_id=0, changeset_id=2, metas=[md(FILE_PATH, "/tmp/file2")]
    )

    changes = await ctx.upsert(
        actor_id=0,
        changeset_id=3,
        metas=[md(FILE_PATH, "/tmp/file2"), md(FILE_PATH, "/tmp/file3")],
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_rows(FILE_PATH)
    assert len(rows) == 3
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].changeset_id == 1
    assert rows[1].value_text == "/tmp/file2"
    assert rows[1].removed is False
    assert rows[1].changeset_id == 2
    assert rows[2].value_text == "/tmp/file3"
    assert rows[2].removed is False
    assert rows[2].changeset_id == 3


@pytest.mark.asyncio
async def test_upsert_to_remove_one_value(ctx: UpsertFixture):
    await ctx.add_initial(
        actor_id=0, changeset_id=1, metas=[md(FILE_PATH, "/tmp/file1")]
    )
    await ctx.add_initial(
        actor_id=0, changeset_id=2, metas=[md(FILE_PATH, "/tmp/file2")]
    )

    changes = await ctx.upsert(
        actor_id=1,
        changeset_id=3,
        metas=[md(FILE_PATH, "/tmp/file1", removed=True), md(FILE_PATH, "/tmp/file2")],
    )

    assert {FILE_PATH} == changes
    rows = await ctx.fetch_rows(FILE_PATH)
    assert len(rows) == 4
    assert rows[0].value_text == "/tmp/file1"
    assert rows[0].removed is False
    assert rows[0].changeset_id == 1
    assert rows[1].value_text == "/tmp/file2"
    assert rows[1].removed is False
    assert rows[1].changeset_id == 2
    assert rows[2].value_text == "/tmp/file1"
    assert rows[2].removed is True
    assert rows[2].changeset_id == 3
    assert rows[3].value_text == "/tmp/file2"
    assert rows[3].removed is False
    assert rows[3].changeset_id == 3
