from __future__ import annotations

import pytest

from katalog.api.helpers import ApiError
from katalog.api.operations import authorize_source
from katalog.db.actors import get_actor_repo
from katalog.models import ActorType
from katalog.sources.fake_assets import FakeAssetSource


@pytest.mark.asyncio
async def test_authorize_source_returns_authorization_url(db_session, monkeypatch):
    _ = db_session
    actor_repo = get_actor_repo()
    actor = await actor_repo.create(
        name="auth-source",
        plugin_id=FakeAssetSource.plugin_id,
        type=ActorType.SOURCE,
        config={"namespace": "auth-source", "total_assets": 1},
    )
    assert actor.id is not None

    monkeypatch.setattr(
        FakeAssetSource,
        "authorize",
        lambda self, **kwargs: "https://example.com/oauth/start",
    )

    result = await authorize_source(int(actor.id))
    assert result["status"] == "authorization_required"
    assert result["authorization_url"] == "https://example.com/oauth/start"


@pytest.mark.asyncio
async def test_authorize_source_returns_authorized_when_no_redirect_needed(
    db_session, monkeypatch
):
    _ = db_session
    actor_repo = get_actor_repo()
    actor = await actor_repo.create(
        name="already-auth-source",
        plugin_id=FakeAssetSource.plugin_id,
        type=ActorType.SOURCE,
        config={"namespace": "already-auth", "total_assets": 1},
    )
    assert actor.id is not None

    monkeypatch.setattr(FakeAssetSource, "authorize", lambda self, **kwargs: "authorized")

    result = await authorize_source(int(actor.id))
    assert result["status"] == "authorized"
    assert result["authorization_url"] is None


@pytest.mark.asyncio
async def test_authorize_source_fails_for_disabled_source(db_session):
    _ = db_session
    actor_repo = get_actor_repo()
    actor = await actor_repo.create(
        name="disabled-source",
        plugin_id=FakeAssetSource.plugin_id,
        type=ActorType.SOURCE,
        disabled=True,
        config={"namespace": "disabled-source", "total_assets": 1},
    )
    assert actor.id is not None

    with pytest.raises(ApiError) as exc:
        await authorize_source(int(actor.id))
    assert exc.value.status_code == 409
    assert str(exc.value.detail) == "Source is disabled"


@pytest.mark.asyncio
async def test_authorize_source_fails_for_missing_source(db_session):
    _ = db_session
    with pytest.raises(ApiError) as exc:
        await authorize_source(999999)
    assert exc.value.status_code == 404
    assert str(exc.value.detail) == "Source not found"
