from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Iterable

import pytest
from tortoise import Tortoise

from katalog.metadata import TIME_MODIFIED
from katalog.models import (
    Asset,
    Metadata,
    OpStatus,
    Provider,
    ProviderType,
    Snapshot,
    make_metadata,
)
from katalog.queries import sync_metadata_registry


async def _init_db() -> None:
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": ["katalog.models"]},
        use_tz=False,  # Preserve whatever tzinfo we hand in; no UTC normalization.
    )
    await Tortoise.generate_schemas()
    await sync_metadata_registry()


async def _teardown_db() -> None:
    await Tortoise.close_connections()


async def _seed_records() -> tuple[Provider, Snapshot, Asset]:
    provider = await Provider.create(
        name="tz-provider",
        plugin_id="plugin.tz",
        type=ProviderType.SOURCE,
    )
    snapshot = await Snapshot.create(provider=provider, status=OpStatus.IN_PROGRESS)
    asset = Asset(
        external_id="asset-tz",
        canonical_uri="file:///asset-tz",
    )
    await asset.save_record(snapshot=snapshot, provider=provider)
    return provider, snapshot, asset


async def _roundtrip(datetimes: Iterable[datetime]) -> list[tuple[datetime, datetime]]:
    provider, snapshot, asset = await _seed_records()
    pairs: list[tuple[datetime, datetime]] = []

    for idx, original in enumerate(datetimes):
        md = make_metadata(
            TIME_MODIFIED,
            original,
            provider_id=provider.id,
            asset=asset,
            snapshot=snapshot,
        )
        md.removed = False
        await md.save()

        fetched = await Metadata.get(id=md.id)
        pairs.append((original, fetched.value_datetime))

    return pairs


@pytest.mark.asyncio
async def test_datetime_roundtrip_preserves_tzinfo() -> None:
    """Verify that we read back the same tz-awareness we wrote."""
    await _init_db()
    try:
        test_values = (
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),  # UTC aware
            datetime(
                2024, 6, 1, 8, 30, 0, tzinfo=timezone(timedelta(hours=9))
            ),  # +09:00
            datetime(
                2024, 6, 1, 8, 30, 0, tzinfo=timezone(timedelta(hours=-5, minutes=-30))
            ),  # -05:30
        )

        pairs = await _roundtrip(test_values)
        for original, roundtripped in pairs:
            assert roundtripped == original, (
                f"Expected {original!r} but got {roundtripped!r}"
            )
    finally:
        await _teardown_db()


@pytest.mark.asyncio
async def test_naive_datetime_is_rejected() -> None:
    await _init_db()
    try:
        provider, snapshot, asset = await _seed_records()
        naive = datetime(2024, 1, 1, 12, 0, 0)
        with pytest.raises(ValueError):
            md = make_metadata(
                TIME_MODIFIED,
                naive,
                provider_id=provider.id,
                asset=asset,
                snapshot=snapshot,
            )
            md.removed = False
            await md.save()
    finally:
        await _teardown_db()
