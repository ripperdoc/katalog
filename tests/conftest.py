from __future__ import annotations

import pytest

from tests.utils.fakes import DatabaseStub


@pytest.fixture()
def database_stub() -> DatabaseStub:
    return DatabaseStub()
