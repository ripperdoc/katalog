from __future__ import annotations


import pytest


@pytest.mark.asyncio
async def test_datetime_roundtrip_preserves_tzinfo() -> None:
    """Verify that if we write a tz-aware datetime to metadata in DB, we get the same back as when we read it again."""


@pytest.mark.asyncio
async def test_naive_datetime_is_rejected() -> None:
    """Verify that attempting to write a naive datetime to metadata raises an error."""
